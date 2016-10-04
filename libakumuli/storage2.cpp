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


#include "storage2.h"
#include "util.h"
#include "queryprocessor.h"
#include "query_processing/queryparser.h"
#include "log_iface.h"

#include <algorithm>
#include <atomic>
#include <sstream>
#include <cassert>
#include <functional>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

namespace Akumuli {

// Utility functions & classes //


// Standalone functions //

/** This function creates metadata file - root of the storage system.
  * This page contains creation date and time, number of pages,
  * all the page file names and they order.
  * @return APR_EINIT on DB error.
  */
static apr_status_t create_metadata_page( const char* file_name
                                        , std::vector<std::string> const& page_file_names)
{
    using namespace std;
    try {
        auto storage = std::make_shared<MetadataStorage>(file_name);

        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);

        storage->init_config(date_time);

        std::vector<MetadataStorage::VolumeDesc> desc;
        int ix = 0;
        for(auto str: page_file_names) {
            desc.push_back(std::make_pair(ix++, str));
        }
        storage->init_volumes(desc);

    } catch (std::exception const& err) {
        std::stringstream fmt;
        fmt << "Can't create metadata file " << file_name << ", the error is: " << err.what();
        Logger::msg(AKU_LOG_ERROR, fmt.str().c_str());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}

//--------- StorageSession ----------

StorageSession::StorageSession(std::shared_ptr<Storage> storage, std::shared_ptr<StorageEngine::CStoreSession> session)
    : storage_(storage)
    , session_(session)
    , matcher_substitute_(nullptr)
{
}

aku_Status StorageSession::write(aku_Sample const& sample) {
    using namespace StorageEngine;
    std::vector<u64> rpoints;
    auto status = session_->write(sample, &rpoints);
    switch (status) {
    case NBTreeAppendResult::OK:
        return AKU_SUCCESS;
    case NBTreeAppendResult::OK_FLUSH_NEEDED:
        storage_-> _update_rescue_points(sample.paramid, std::move(rpoints));
        return AKU_SUCCESS;
    case NBTreeAppendResult::FAIL_BAD_ID:
        AKU_PANIC("Invalid session cache, id = " + std::to_string(sample.paramid));
    case NBTreeAppendResult::FAIL_LATE_WRITE:
        return AKU_ELATE_WRITE;
    case NBTreeAppendResult::FAIL_BAD_VALUE:
        return AKU_EBAD_ARG;
    };
    return AKU_SUCCESS;
}

aku_Status StorageSession::init_series_id(const char* begin, const char* end, aku_Sample *sample) {
    // Series name normalization procedure. Most likeley a bottleneck but
    // can be easily parallelized.
    const char* ksbegin = nullptr;
    const char* ksend = nullptr;
    char buf[AKU_LIMITS_MAX_SNAME];
    char* ob = static_cast<char*>(buf);
    char* oe = static_cast<char*>(buf) + AKU_LIMITS_MAX_SNAME;
    aku_Status status = SeriesParser::to_normal_form(begin, end, ob, oe, &ksbegin, &ksend);
    if (status != AKU_SUCCESS) {
        return status;
    }
    // Match series name locally (on success use local information)
    // Otherwise - match using global registry. On success - add global information to
    //  the local matcher. On error - add series name to global registry and then to
    //  the local matcher.
    u64 id = local_matcher_.match(ob, ksend);
    if (!id) {
        // go to global registery
        status = storage_->init_series_id(ob, ksend, sample, &local_matcher_);
    } else {
        // initialize using local info
        sample->paramid = id;
    }
    return status;
}

int StorageSession::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size) {
    SeriesMatcher::StringT name;
    if (matcher_substitute_) {
        // Use temporary matcher
        name = matcher_substitute_->id2str(id);
        if (name.first == nullptr) {
            // no such id, user error
            return 0;
        }
    } else {
        name = local_matcher_.id2str(id);
        if (name.first == nullptr) {
            // not yet cached!
            return storage_->get_series_name(id, buffer, buffer_size, &local_matcher_);
        }
    }
    memcpy(buffer, name.first, static_cast<size_t>(name.second));
    return name.second;
}

void StorageSession::query(Caller& caller, InternalCursor* cur, const char* query) const {
    storage_->query(this, caller, cur, query);
}

void StorageSession::set_series_matcher(std::shared_ptr<SeriesMatcher> matcher) const {
    matcher_substitute_ = matcher;
}

void StorageSession::clear_series_matcher() const {
    matcher_substitute_ = nullptr;
}

//----------- Storage ----------

Storage::Storage(const char* path)
    : done_{0}
    , close_barrier_(2)
{
    metadata_.reset(new MetadataStorage(path));

    std::string metapath;
    std::vector<std::string> volpaths;

    // first volume is a metavolume
    auto volumes = metadata_->get_volumes();
    for (auto vol: volumes) {
        std::string path;
        int index;
        std::tie(index, path) = vol;
        if (index == 0) {
            metapath = path;
        } else {
            volpaths.push_back(path);
        }
    }

    bstore_ = StorageEngine::FixedSizeFileStorage::open(metapath, volpaths);
    cstore_ = std::make_shared<StorageEngine::ColumnStore>(bstore_);

    start_sync_worker();
}

Storage::Storage(std::shared_ptr<MetadataStorage>                  meta,
                 std::shared_ptr<StorageEngine::BlockStore>        bstore,
                 std::shared_ptr<StorageEngine::ColumnStore>       cstore,
                 bool                                              start_worker)
    : bstore_(bstore)
    , cstore_(cstore)
    , done_{0}
    , close_barrier_(2)
    , metadata_(meta)
{
    if (start_worker) {
        start_sync_worker();
    }
}

void Storage::start_sync_worker() {
    // This thread periodically sync rescue points and series names.
    // It calls `flush` method of the blockstore and then `sync_with_metadata_storage` method
    // if something needs to be synced.
    // This order guarantees that metadata storage always contains correct rescue points and
    // other metadata.
    auto sync_worker = [this]() {
        auto get_names = [this](std::vector<SeriesMatcher::SeriesNameT>* names) {
            std::lock_guard<std::mutex> guard(lock_);
            global_matcher_.pull_new_names(names);
        };

        while(done_.load() == 0) {
            auto status = metadata_->wait_for_sync_request(1000);
            if (status == AKU_SUCCESS) {
                bstore_->flush();
                metadata_->sync_with_metadata_storage(get_names);
            }
        }

        close_barrier_.wait();
    };
    std::thread sync_worker_thread(sync_worker);
    sync_worker_thread.detach();

}

void Storage::close() {
    // Wait for all ingestion sessions to stop
    done_.store(1);
    close_barrier_.wait();
}


void Storage::_update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& rpoints) {
    metadata_->add_rescue_point(id, std::move(rpoints));
}

std::shared_ptr<StorageSession> Storage::create_write_session() {
    std::shared_ptr<StorageEngine::CStoreSession> session = std::make_shared<StorageEngine::CStoreSession>(cstore_);
    return std::make_shared<StorageSession>(shared_from_this(), session);
}

aku_Status Storage::init_series_id(const char* begin, const char* end, aku_Sample *sample, SeriesMatcher *local_matcher) {
    u64 id = 0;
    bool create_new = false;
    {
        std::lock_guard<std::mutex> guard(lock_);
        id = global_matcher_.match(begin, end);
        if (id == 0) {
            // create new series
            id = global_matcher_.add(begin, end);
            metadata_->add_rescue_point(id, std::vector<u64>());
            create_new = true;
        }
    }
    if (create_new) {
        // id guaranteed to be unique
        cstore_->create_new_column(id);
    }
    sample->paramid = id;
    local_matcher->_add(begin, end, id);
    return AKU_SUCCESS;
}

int Storage::get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, SeriesMatcher *local_matcher) {
    std::lock_guard<std::mutex> guard(lock_);
    auto str = global_matcher_.id2str(id);
    if (str.first == nullptr) {
        return 0;
    }
    // copy value to local matcher
    local_matcher->_add(str.first, str.first + str.second, id);
    // copy the string to out buffer
    if (str.second > static_cast<int>(buffer_size)) {
        return -1*str.second;
    }
    memcpy(buffer, str.first, static_cast<size_t>(str.second));
    return str.second;
}

void Storage::query(StorageSession const* session, Caller& caller, InternalCursor* cur, const char* query) const {
    using namespace QP;
    std::lock_guard<std::mutex> guard(lock_);
    boost::property_tree::ptree ptree;
    aku_Status status;
    std::tie(status, ptree) = QueryParser::parse_json(query);
    if (status != AKU_SUCCESS) {
        cur->set_error(caller, status);
        return;
    }
    QueryKind kind;
    std::tie(status, kind) = QueryParser::get_query_kind(ptree);
    if (status != AKU_SUCCESS) {
        cur->set_error(caller, status);
        return;
    }
    if (kind == QueryKind::SELECT) {
        std::vector<aku_ParamId> ids;
        std::tie(status, ids) = QueryParser::parse_select_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            cur->set_error(caller, status);
            return;
        }
        GroupByTime tm;
        std::vector<std::shared_ptr<Node>> nodes;
        std::tie(status, tm, nodes) = QueryParser::parse_processing_topology(ptree, caller, cur);
        if (status != AKU_SUCCESS) {
            cur->set_error(caller, status);
            return;
        }
        AKU_UNUSED(tm);
        std::shared_ptr<IStreamProcessor> proc = std::make_shared<MetadataQueryProcessor>(nodes.front(), std::move(ids));
        if (proc->start()) {
            proc->stop();
        }
    } else if (kind == QueryKind::AGGREGATE) {
        AKU_PANIC("Not implemented");
    } else if (kind == QueryKind::SCAN) {
        ReshapeRequest req;
        std::tie(status, req) = QueryParser::parse_scan_query(ptree, global_matcher_);
        if (status != AKU_SUCCESS) {
            cur->set_error(caller, status);
            return;
        }
        std::vector<std::shared_ptr<Node>> nodes;
        GroupByTime groupbytime;
        std::tie(status, groupbytime, nodes) = QueryParser::parse_processing_topology(ptree, caller, cur);
        if (status != AKU_SUCCESS) {
            cur->set_error(caller, status);
            return;
        }
        std::shared_ptr<IStreamProcessor> proc = std::make_shared<ScanQueryProcessor>(nodes, groupbytime);
        if (req.group_by.enabled) {
            session->set_series_matcher(req.group_by.matcher);
        } else {
            session->clear_series_matcher();
        }
        // Scan column store
        if (proc->start()) {
            cstore_->query(req, *proc);
            proc->stop();
        }
    }
}

void Storage::debug_print() const {
    std::cout << "Storage::debug_print" << std::endl;
    std::cout << "...not implemented" << std::endl;
}

aku_Status Storage::new_database( const char     *file_name
                                , const char     *metadata_path
                                , const char     *volumes_path
                                , i32             num_volumes
                                , u64             page_size)
{
    // Create volumes and metapage
    u32 vol_size = static_cast<u32>(page_size / 4096);

    boost::filesystem::path volpath(volumes_path);
    boost::filesystem::path metpath(metadata_path);
    volpath = boost::filesystem::absolute(volpath);
    metpath = boost::filesystem::absolute(metpath);
    std::string sqlitebname = std::string(file_name) + ".akumuli";
    boost::filesystem::path sqlitepath = metpath / sqlitebname;

    if (!boost::filesystem::exists(volpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(volumes_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(volpath);
    } else {
        if (!boost::filesystem::is_directory(volpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(volumes_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    if (!boost::filesystem::exists(metpath)) {
        Logger::msg(AKU_LOG_INFO, std::string(metadata_path) + " doesn't exists, trying to create directory");
        boost::filesystem::create_directories(metpath);
    } else {
        if (!boost::filesystem::is_directory(metpath)) {
            Logger::msg(AKU_LOG_ERROR, std::string(metadata_path) + " is not a directory");
            return AKU_EBAD_ARG;
        }
    }

    if (boost::filesystem::exists(sqlitepath)) {
        Logger::msg(AKU_LOG_ERROR, "Database is already exists");
        return AKU_EBAD_ARG;
    }

    std::vector<std::tuple<u32, std::string>> paths;
    for (i32 i = 0; i < num_volumes; i++) {
        std::string basename = std::string(file_name) + "_" + std::to_string(i) + ".vol";
        boost::filesystem::path p = volpath / basename;
        paths.push_back(std::make_tuple(vol_size, p.string()));
    }
    // Volumes meta-page
    std::string basename = std::string(file_name) + ".metavol";
    boost::filesystem::path volmpage = volpath / basename;

    StorageEngine::FixedSizeFileStorage::create(volmpage.string(), paths);

    // Create sqlite database for metadata
    std::vector<std::string> mpaths;
    mpaths.push_back(volmpage.string());
    for (auto p: paths) {
        mpaths.push_back(std::get<1>(p));
    }
    create_metadata_page(sqlitepath.c_str(), mpaths);
    return AKU_SUCCESS;
}

aku_Status Storage::remove_storage(const char* file_name, bool force) {
    if (!boost::filesystem::exists(file_name)) {
        return AKU_ENOT_FOUND;
    }
    auto meta = std::make_shared<MetadataStorage>(file_name);
    auto volumes = meta->get_volumes();
    if (volumes.empty()) {
        // Bad database
        return AKU_EBAD_ARG;
    }
    std::vector<std::string> volume_names(volumes.size() - 1, "");
    // First volume is meta-page
    std::string meta_file;
    for(auto it: volumes) {
        if (it.first == 0) {
            meta_file = it.second;
        } else {
            volume_names.at(static_cast<size_t>(it.first) - 1) = it.second;
        }
    }
    if (!force) {
        // Check whether or not database is empty
        auto fstore = StorageEngine::FixedSizeFileStorage::open(meta_file, volume_names);
        auto stats = fstore->get_stats();
        if (stats.nblocks != 0) {
            // DB is not empty
            return AKU_ENOT_PERMITTED;
        }
    }
    meta.reset();

    // Check access rights
    auto check_access = [](std::string const& p) {
        auto status = boost::filesystem::status(p);
        auto perms = status.permissions();
        if ((perms & boost::filesystem::owner_write) == 0) {
            return AKU_EACCESS;
        }
        return AKU_SUCCESS;
    };
    volume_names.push_back(meta_file);
    volume_names.push_back(file_name);
    std::vector<aku_Status> statuses;
    std::transform(volume_names.begin(), volume_names.end(), std::back_inserter(statuses), check_access);
    auto comb_status = [](aku_Status lhs, aku_Status rhs) {
        return lhs == AKU_SUCCESS ? rhs : lhs;
    };
    auto status = std::accumulate(statuses.begin(), statuses.end(), AKU_SUCCESS, comb_status);
    if (status != AKU_SUCCESS) {
        return AKU_EACCESS;
    }

    // Actual deletion starts here!
    auto delete_file = [](std::string const& fname) {
        if (!boost::filesystem::remove(fname)) {
            Logger::msg(AKU_LOG_ERROR, fname + " file is not deleted!");
        } else {
            Logger::msg(AKU_LOG_INFO, fname + " was deleted.");
        }
    };

    std::for_each(volume_names.begin(), volume_names.end(), delete_file);

    return AKU_SUCCESS;
}

}
