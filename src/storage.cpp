/**
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#include "storage.h"
#include "util.h"
#include "cursor.h"

#include <cstdlib>
#include <cstdarg>
#include <stdexcept>
#include <algorithm>
#include <new>
#include <atomic>
#include <sstream>
#include <cassert>
#include <functional>

#include <apr_general.h>
#include <apr_mmap.h>
#include <apr_xml.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_array.hpp>

namespace Akumuli {

//----------------------------------Volume----------------------------------------------

// TODO: remove max_cache_size
Volume::Volume(const char* file_name, aku_Duration window, size_t max_cache_size, int tag, aku_printf_t logger)
    : mmap_(file_name, tag, logger)
    , window_(window)
    , max_cache_size_(max_cache_size)
{
    mmap_.panic_if_bad();  // panic if can't mmap volume
    page_ = reinterpret_cast<PageHeader*>(mmap_.get_pointer());
    cache_.reset(new Sequencer(page_, window_));
}

PageHeader* Volume::get_page() const {
    return page_;
}

PageHeader* Volume::reallocate_disc_space() {
    uint32_t page_id = page_->page_id;
    uint32_t open_count = page_->open_count;
    uint32_t close_count = page_->close_count;
    mmap_.remap_file_destructive();
    page_ = new (mmap_.get_pointer()) PageHeader(0, mmap_.get_size(), page_id);
    page_->open_count = open_count;
    page_->close_count = close_count;
    return page_;
}

void Volume::open() {
    page_->reuse();
    mmap_.flush();
}

void Volume::close() {
    page_->close();
    mmap_.flush();
}

//----------------------------------Storage---------------------------------------------

static std::atomic<int> storage_cnt = {1};

Storage::Storage(const char* path, aku_Config const& conf)
    : tag_(storage_cnt++)
{
    aku_printf_t logger = conf.logger;
    if (logger == nullptr) {
        logger = &aku_console_logger;
    }
    logger_ = logger;

    /* Exception, thrown from this c-tor means that something really bad
     * happend and we it's impossible to open this storage, for example -
     * because metadata file is corrupted, or volume is missed on disc.
     */

    ttl_= conf.max_late_write;

    // NOTE: incremental backup target will be stored in metadata file

    // TODO: use xml (apr_xml.h) instead of json because boost::property_tree json parsing
    //       is FUBAR and uses boost::spirit.

    // 1. Read json file
    boost::property_tree::ptree ptree;
    // NOTE: there is a known bug in boost 1.49 - https://svn.boost.org/trac/boost/ticket/6785
    // FIX: sed -i -e 's/std::make_pair(c.name, Str(b, e))/std::make_pair(c.name, Ptree(Str(b, e)))/' json_parser_read.hpp
    boost::property_tree::json_parser::read_json(path, ptree);

    // 2. Read volumes
    int num_volumes = ptree.get_child("num_volumes").get_value(0);
    if (num_volumes == 0) {
        AKU_PANIC("invalid storage");
    }

    std::vector<std::string> volume_names(num_volumes);
    for(auto child_node: ptree.get_child("volumes")) {
        auto volume_index = child_node.second.get_child("index").get_value_optional<int>();
        auto volume_path = child_node.second.get_child("path").get_value_optional<std::string>();
        if (volume_index && volume_path) {
            volume_names.at(*volume_index) = *volume_path;
        }
        else {
            AKU_PANIC("invalid storage, bad volume link");
        }
    }

    // check result
    for(std::string const& path: volume_names) {
        if (path.empty())
            AKU_PANIC("invalid storage, one of the volumes is missing");
    }

    // create volumes list
    for(auto path: volume_names) {
        // TODO: convert conf.max_cache_size from bytes
        Volume* vol = new Volume(path.c_str(), ttl_, conf.max_cache_size, tag_, logger_);
        volumes_.push_back(vol);
    }

    select_active_page();

    prepopulate_cache(conf.max_cache_size);
}

void Storage::select_active_page() {
    // volume with max overwrites_count and max index must be active
    int max_index = -1;
    int64_t max_overwrites = -1;
    for(int i = 0; i < (int)volumes_.size(); i++) {
        PageHeader* page = volumes_.at(i)->get_page();
        if (static_cast<int64_t>(page->open_count) >= max_overwrites) {
            max_overwrites = static_cast<int64_t>(page->open_count);
            max_index = i;
        }
        prefetch_mem(page->histogram.entries, sizeof(page->histogram.entries));
    }

    active_volume_index_ = max_index;
    active_volume_ = volumes_.at(max_index);
    active_page_ = active_volume_->get_page();

    if (active_page_->close_count == active_page_->open_count) {
        // Application was interrupted during volume
        // switching procedure
        advance_volume_(active_volume_index_.load());
    }
}

void Storage::prepopulate_cache(int64_t max_cache_size) {
    // All entries between sync_index (included) and count must
    // be cached.
    auto begin = active_page_->sync_count;
    auto end = active_page_->count;

    // use 1Mb of cache by default
    while(begin < end) {
        const aku_Entry* entry = active_page_->read_entry_at(begin);
        auto off_err = active_page_->index_to_offset(begin);
        if (off_err.second != AKU_SUCCESS) {
            continue;
        }
        TimeSeriesValue ts_value(entry->time, entry->param_id, off_err.first);
        int add_status;
        Sequencer::Lock merge_lock;
        std::tie(add_status, merge_lock) = active_volume_->cache_->add(ts_value);
        if (merge_lock.owns_lock()) {
            Caller caller;
            DirectPageSyncCursor cursor(rand_);
            active_volume_->cache_->merge(caller, &cursor, std::move(merge_lock));
        }
        begin++;
    }
}

void Storage::advance_volume_(int local_rev) {
    // TODO: transfer baseline_ value from old volume to new
    if (local_rev == active_volume_index_.load()) {
        active_volume_->close();
        // select next page in round robin order
        active_volume_index_++;
        active_volume_ = volumes_[active_volume_index_ % volumes_.size()];
        active_page_ = active_volume_->reallocate_disc_space();
        active_volume_->open();
    }
    // Or other thread already done all the switching
    // just redo all the things
}

void Storage::log_error(const char* message) {
    (*logger_)(tag_, "Write error: %s", message);
}

// Reading

void Storage::search(Caller &caller, InternalCursor *cur, const SearchQuery &query) const {
    // Find pages
    // at this stage of development - simply get all pages :)
    std::vector<std::unique_ptr<ExternalCursor>> cursors;
    for(auto vol: volumes_) {
        // Search cache (optional, only for active page)
        if (vol == this->active_volume_) {
            //auto ccur = CoroCursor::make(&Sequencer::search, this->active_volume_->cache_.get(), query);
            //cursors.push_back(std::move(ccur));
        }
        // Search pages
        auto pcur = CoroCursor::make(&PageHeader::search, vol->page_, query);
        cursors.push_back(std::move(pcur));
    }

    std::vector<ExternalCursor*> pcursors;
    std::transform( cursors.begin(), cursors.end()
                  , std::back_inserter(pcursors)
                  , [](std::unique_ptr<ExternalCursor>& v) { return v.get(); });

    assert(pcursors.size());
    FanInCursorCombinator fan_in_cursor(&pcursors[0], pcursors.size(), query.direction);

    // TODO: remove excessive copying
    // to do this I need to pass cur to fan_in_cursor somehow
    const int results_len = 0x1000;
    CursorResult results[results_len];
    while(!fan_in_cursor.is_done()) {
        int s = fan_in_cursor.read(results, results_len);
        int err_code = 0;
        if (fan_in_cursor.is_error(&err_code)) {
            cur->set_error(caller, err_code);
            return;
        }
        for (int i = 0; i < s; i++) {
            cur->put(caller, results[i].first, results[i].second);
        }
    }
    fan_in_cursor.close();
    cur->complete(caller);
}

void Storage::get_stats(aku_StorageStats* rcv_stats) {
    uint64_t used_space = 0,
             free_space = 0,
              n_entries = 0;

    for (Volume const* vol: volumes_) {
        auto all = vol->page_->length;
        auto free = vol->page_->get_free_space();
        used_space += all - free;
        free_space += free;
        n_entries += vol->page_->count;
    }
    rcv_stats->n_volumes = volumes_.size();
    rcv_stats->free_space = free_space;
    rcv_stats->used_space = used_space;
    rcv_stats->n_entries = n_entries;
}

// Writing

//! commit changes
void Storage::commit() {
    // TODO: volume->flush()
}

//! write data
aku_Status Storage::write(aku_ParamId param, aku_TimeStamp ts, aku_MemRange data) {
    int status = AKU_WRITE_STATUS_BAD_DATA;
    while(true) {
        int local_rev = active_volume_index_.load();
        status = active_page_->add_entry(param, ts, data);
        switch (status) {
        case AKU_SUCCESS: {
            TimeSeriesValue ts_value(ts, param, active_page_->last_offset);
            Sequencer::Lock merge_lock;
            std::tie(status, merge_lock) = active_volume_->cache_->add(ts_value);
            if (merge_lock.owns_lock()) {
                // Slow path
                Caller caller;
                DirectPageSyncCursor cursor(rand_);
                active_volume_->cache_->merge(caller, &cursor, std::move(merge_lock));
            }
            return status;
        }
        case AKU_EOVERFLOW:
            advance_volume_(local_rev);
            break;  // retry
        case AKU_ELATE_WRITE:
        // Branch for rare and unexpected errors
        default:
            log_error(aku_error_message(status));
            return status;
        };
    }
}



/** This function creates file with specified size
  */
static apr_status_t create_file(const char* file_name, uint64_t size, aku_printf_t logger) {
    apr_status_t status;
    int success_count = 0;
    apr_pool_t* mem_pool = NULL;
    apr_file_t* file = NULL;

    status = apr_pool_create(&mem_pool, NULL);
    if (status == APR_SUCCESS) {
        success_count++;

        // Create new file
        status = apr_file_open(&file, file_name, APR_CREATE|APR_WRITE, APR_OS_DEFAULT, mem_pool);
        if (status == APR_SUCCESS) {
            success_count++;

            // Truncate file
            status = apr_file_trunc(file, size);
            if (status == APR_SUCCESS)
                success_count++;
        }
    }

    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        (*logger)(0, "Can't create file, error %s on step %d", error_message, success_count);
    }

    switch(success_count) {
    case 3:
    case 2:
        status = apr_file_close(file);
    case 1:
        apr_pool_destroy(mem_pool);
    case 0:
        // even apr pool is not created
        break;
    }
    return status;
}


/** This function creates one of the page files with specified
  * name and index.
  */
static apr_status_t create_page_file(const char* file_name, uint32_t page_index, aku_printf_t logger) {
    apr_status_t status;
    int64_t size = AKU_MAX_PAGE_SIZE;

    status = create_file(file_name, size, logger);
    if (status != APR_SUCCESS) {
        (*logger)(0, "Can't create page file %s", file_name);
        return status;
    }

    MemoryMappedFile mfile(file_name, 0, logger);
    if (mfile.is_bad())
        return mfile.status_code();

    // Create index page
    auto index_ptr = mfile.get_pointer();
    auto index_page = new (index_ptr) PageHeader(0, AKU_MAX_PAGE_SIZE, page_index);

    // FIXME: revisit - is it actually needed?
    // Activate the first page
    if (page_index == 0) {
        index_page->reuse();
    }
    return status;
}

/** Create page files, return list of statuses.
  */
static std::vector<apr_status_t> create_page_files(std::vector<std::string> const& targets, aku_printf_t logger) {
    std::vector<apr_status_t> results(targets.size(), APR_SUCCESS);
    for (size_t ix = 0; ix < targets.size(); ix++) {
        apr_status_t res = create_page_file(targets[ix].c_str(), ix, logger);
        results[ix] = res;
    }
    return results;
}

static std::vector<apr_status_t> delete_files(const std::vector<std::string>& targets, const std::vector<apr_status_t>& statuses, aku_printf_t logger) {
    if (targets.size() != statuses.size()) {
        AKU_PANIC("sizes of targets and statuses doesn't match");
    }
    apr_pool_t* mem_pool = NULL;
    int op_count = 0;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    std::vector<apr_status_t> results;
    if (status == APR_SUCCESS) {
        op_count++;
        for(auto ix = 0u; ix < targets.size(); ix++) {
            const std::string& target = targets[ix];
            if (statuses[ix] == APR_SUCCESS) {
                (*logger)(0, "Removing %s", target.c_str());
                status = apr_file_remove(target.c_str(), mem_pool);
                results.push_back(status);
                if (status != APR_SUCCESS) {
                    char error_message[1024];
                    apr_strerror(status, error_message, 1024);
                    (*logger)(0, "Error [%s] while deleting a file %s", error_message, target.c_str());
                }
            }
            else {
                (*logger)(0, "Target %s doesn't need to be removed", target.c_str());
            }
        }
    }
    if (op_count) {
        apr_pool_destroy(mem_pool);
    }
    return results;
}


/** This function creates metadata file - root of the storage system.
  * This page contains creation date and time, number of pages,
  * all the page file names and they order.
  */
static apr_status_t create_metadata_page( const char* file_name
                                        , std::vector<std::string> const& page_file_names
                                        , aku_printf_t logger)
{
    // TODO: use xml (apr_xml.h) instead of json because boost::property_tree json parsing
    //       is FUBAR and uses boost::spirit.
    try {
        boost::property_tree::ptree root;
        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);
        root.add("creation_time", date_time);
        root.add("num_volumes", page_file_names.size());
        boost::property_tree::ptree volumes_list;
        for(size_t i = 0; i < page_file_names.size(); i++) {
            boost::property_tree::ptree page_desc;
            page_desc.add("index", i);
            page_desc.add("path", page_file_names[i]);
            volumes_list.push_back(std::make_pair("", page_desc));
        }
        root.add_child("volumes", volumes_list);
        boost::property_tree::json_parser::write_json(file_name, root);
    }
    catch(const std::exception& err) {
        (*logger)(0, "Can't generate JSON file %s, the error is: %s", file_name, err.what());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}


apr_status_t Storage::new_storage( const char* 	file_name
                                 , const char* 	metadata_path
                                 , const char* 	volumes_path
                                 , int          num_pages
                                 , aku_printf_t logger
                                 )
{
    apr_pool_t* mempool;
    apr_status_t status = apr_pool_create(&mempool, NULL);
    if (status != APR_SUCCESS)
        return status;

    // calculate list of page-file names
    std::vector<std::string> page_names;
    for (int ix = 0; ix < num_pages; ix++) {
        std::stringstream stream;
        stream << file_name << "_" << ix << ".volume";
        char* path = nullptr;
        std::string volume_file_name = stream.str();
        status = apr_filepath_merge(&path, volumes_path, volume_file_name.c_str(), APR_FILEPATH_NATIVE, mempool);
        if (status != APR_SUCCESS) {
            auto error_message = apr_error_message(status);
            (*logger)(0, "Invalid volumes path: %s", error_message.c_str());
            apr_pool_destroy(mempool);
            AKU_APR_PANIC(status, error_message.c_str());
        }
        page_names.push_back(path);
    }

    apr_pool_clear(mempool);

    status = apr_dir_make(metadata_path, APR_OS_DEFAULT, mempool);
    if (status == APR_EEXIST) {
        (*logger)(0, "Metadata dir already exists");
    }
    status = apr_dir_make(volumes_path, APR_OS_DEFAULT, mempool);
    if (status == APR_EEXIST) {
        (*logger)(0, "Volumes dir already exists");
    }

    std::vector<apr_status_t> page_creation_statuses = create_page_files(page_names, logger);
    for(auto creation_status: page_creation_statuses) {
        if (creation_status != APR_SUCCESS) {
            (*logger)(0, "Not all pages successfullly created. Cleaning up.");
            apr_pool_destroy(mempool);
            delete_files(page_names, page_creation_statuses, logger);
            return creation_status;
        }
    }

    std::stringstream stream;
    stream << file_name << ".akumuli";
    char* path = nullptr;
    std::string metadata_file_name = stream.str();
    status = apr_filepath_merge(&path, metadata_path, metadata_file_name.c_str(), APR_FILEPATH_NATIVE, mempool);
    if (status != APR_SUCCESS) {
        auto error_message = apr_error_message(status);
        (*logger)(0, "Invalid metadata path: %s", error_message.c_str());
        apr_pool_destroy(mempool);
        AKU_APR_PANIC(status, error_message.c_str());
    }
    status = create_metadata_page(path, page_names, logger);
    apr_pool_destroy(mempool);
    return status;
}

}
