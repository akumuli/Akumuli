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
#include <sstream>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_array.hpp>
#include <boost/lexical_cast.hpp>

namespace Akumuli {

static apr_status_t create_page_file(const char* file_name, uint32_t page_index, aku_logger_cb_t logger);

void delete_apr_pool(apr_pool_t *p) {
    if (p) {
        apr_pool_destroy(p);
    }
}

AprHandleDeleter::AprHandleDeleter(const apr_dbd_driver_t *driver)
    : driver(driver)
{
}

void AprHandleDeleter::operator()(apr_dbd_t* handle) {
    if (driver != nullptr && handle != nullptr) {
        apr_dbd_close(driver, handle);
    }
}


//-------------------------------MetadataStorage----------------------------------------

MetadataStorage::MetadataStorage(const char* db, aku_logger_cb_t logger)
    : pool_(nullptr, &delete_apr_pool)
    , driver_(nullptr)
    , handle_(nullptr, AprHandleDeleter(nullptr))
    , logger_(logger)
{
    apr_pool_t *pool = nullptr;
    auto status = apr_pool_create(&pool, NULL);
    if (status != APR_SUCCESS) {
        // report error (can't return error from c-tor)
        throw std::runtime_error("Can't create memory pool");
    }
    pool_.reset(pool);

    status = apr_dbd_get_driver(pool, "sqlite3", &driver_);
    if (status != APR_SUCCESS) {
        (*logger_)(AKU_LOG_ERROR, "Can't load driver, maybe libaprutil1-dbd-sqlite3 isn't installed");
        throw std::runtime_error("Can't load sqlite3 dirver");
    }

    apr_dbd_t *handle = nullptr;
    status = apr_dbd_open(driver_, pool, db, &handle);
    if (status != APR_SUCCESS) {
        (*logger_)(AKU_LOG_ERROR, "Can't open database, check file path");
        throw std::runtime_error("Can't open database");
    }
    handle_ = HandleT(handle, AprHandleDeleter(driver_));

    create_tables();

    // Create prepared statement
    const char* query = "INSERT INTO akumuli_series (series_id, keyslist, storage_id) VALUES (%s, %s, %d)";
    status = apr_dbd_prepare(driver_, pool_.get(), handle_.get(), query, "INSERT_SERIES_NAME", &insert_);
    if (status != 0) {
        (*logger_)(AKU_LOG_ERROR, "Error creating prepared statement");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
}

int MetadataStorage::execute_query(std::string query) {
    if (logger_ != &aku_console_logger) {
        (*logger_)(AKU_LOG_TRACE, query.c_str());
    }
    int nrows = -1;
    int status = apr_dbd_query(driver_, handle_.get(), &nrows, query.c_str());
    if (status != 0 && status != 21) {
        // generate error and throw
        (*logger_)(AKU_LOG_ERROR, "Error executing query");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
    return nrows;
}

void MetadataStorage::create_tables() {
    const char* query = nullptr;

    // Create volumes table
    query =
            "CREATE TABLE IF NOT EXISTS akumuli_volumes("
            "id INTEGER UNIQUE,"
            "path TEXT UNIQUE"
            ");";
    execute_query(query);

    // Create configuration table (key-value-commmentary)
    query =
            "CREATE TABLE IF NOT EXISTS akumuli_configuration("
            "name TEXT UNIQUE,"
            "value TEXT,"
            "comment TEXT"
            ");";
    execute_query(query);

    query =
            "CREATE TABLE IF NOT EXISTS akumuli_series("
            "id INTEGER PRIMARY KEY UNIQUE,"
            "series_id TEXT,"
            "keyslist TEXT,"
            "storage_id INTEGER UNIQUE"
            ");";
    execute_query(query);
}

void MetadataStorage::init_config(uint32_t compression_threshold,
                                  uint32_t max_cache_size,
                                  uint64_t window_size,
                                  const char* creation_datetime)
{
    // Create table and insert data into it

    std::stringstream insert;
    insert << "INSERT INTO akumuli_configuration (name, value, comment)" << std::endl;
    insert << "\tSELECT 'compression_threshold' as name, '" << compression_threshold << "' as value, "
           << "'Compression threshold value' as comment" << std::endl;
    insert << "\tUNION SELECT 'max_cache_size', '" << max_cache_size
           << "', 'Maximal cache size'" << std::endl;
    insert << "\tUNION SELECT 'window_size', '" << window_size << "', 'Write window size'" << std::endl;
    insert << "\tUNION SELECT 'creation_time', '" << creation_datetime
           << "', 'Database creation time'" << std::endl;
    std::string insert_query = insert.str();
    execute_query(insert_query);
}

void MetadataStorage::get_configs(uint32_t *compression_threshold,
                                  uint32_t *max_cache_size,
                                  uint64_t *window_size,
                                  std::string *creation_datetime)
{
    {   // Read compression_threshold
        std::string query = "SELECT value FROM akumuli_configuration "
                            "WHERE name='compression_threshold'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (compression_threshold)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (compression_threshold)");
        }
        *compression_threshold = boost::lexical_cast<uint32_t>(tuple.at(0));
    }
    {   // Read max_cache_size
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='max_cache_size'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (max_cache_size)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (max_cache_size)");
        }
        *max_cache_size = boost::lexical_cast<uint32_t>(tuple.at(0));
    }
    {   // Read window_size
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='window_size'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (window_size)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (window_size)");
        }
        // This value can be encoded as dobule by the sqlite engine
        *window_size = boost::lexical_cast<uint64_t>(tuple.at(0));
    }
    {   // Read creation time
        std::string query = "SELECT value FROM akumuli_configuration WHERE name='creation_time'";
        auto results = select_query(query.c_str());
        if (results.size() != 1) {
            throw std::runtime_error("Invalid configuration (creation_time)");
        }
        auto tuple = results.at(0);
        if (tuple.size() != 1) {
            throw std::runtime_error("Invalid configuration query (creation_time)");
        }
        // This value can be encoded as dobule by the sqlite engine
        *creation_datetime = tuple.at(0);
    }
}

void MetadataStorage::init_volumes(std::vector<VolumeDesc> volumes) {
    std::stringstream query;
    query << "INSERT INTO akumuli_volumes (id, path)" << std::endl;
    bool first = true;
    for (auto desc: volumes) {
        if (first) {
            query << "\tSELECT " << desc.first << " as id, '" << desc.second << "' as path" << std::endl;
            first = false;
        } else {
            query << "\tUNION SELECT " << desc.first << ", '" << desc.second << "'" << std::endl;
        }
    }
    std::string full_query = query.str();
    execute_query(full_query);
}


std::vector<MetadataStorage::UntypedTuple> MetadataStorage::select_query(const char* query) const {
    (*logger_)(AKU_LOG_TRACE, query);
    std::vector<UntypedTuple> tuples;
    apr_dbd_results_t *results = nullptr;
    int status = apr_dbd_select(driver_, pool_.get(), handle_.get(), &results, query, 0);
    if (status != 0) {
        (*logger_)(AKU_LOG_ERROR, "Error executing query");
        throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
    }
    // get rows
    int ntuples = apr_dbd_num_tuples(driver_, results);
    int ncolumns = apr_dbd_num_cols(driver_, results);
    for (int i = ntuples; i --> 0;) {
        apr_dbd_row_t *row = nullptr;
        status = apr_dbd_get_row(driver_, pool_.get(), results, &row, -1);
        if (status != 0) {
            (*logger_)(AKU_LOG_ERROR, "Error getting row from resultset");
            throw std::runtime_error(apr_dbd_error(driver_, handle_.get(), status));
        }
        UntypedTuple tup;
        for (int col = 0; col < ncolumns; col++) {
            const char* entry = apr_dbd_get_entry(driver_, row, col);
            if (entry) {
                tup.emplace_back(entry);
            } else {
                tup.emplace_back();
            }
        }
        tuples.push_back(std::move(tup));
    }
    return tuples;
}

std::vector<MetadataStorage::VolumeDesc> MetadataStorage::get_volumes() const {
    const char* query =
            "SELECT id, path FROM akumuli_volumes;";
    std::vector<VolumeDesc> tuples;
    std::vector<UntypedTuple> untyped = select_query(query);
    // get rows
    auto ntuples = untyped.size();
    for (size_t i = 0; i < ntuples; i++) {
        // get id
        std::string idrepr = untyped.at(i).at(0);
        int id = boost::lexical_cast<int>(idrepr);
        // get path
        std::string path = untyped.at(i).at(1);
        tuples.push_back(std::make_pair(id, path));
    }
    return tuples;
}

struct LightweightString {
    const char* str;
    int len;
};

std::ostream& operator << (std::ostream& s, LightweightString val) {
    s.write(val.str, val.len);
    return s;
}

static bool split_series(const char* str, int n, LightweightString* outname, LightweightString* outkeys) {
    int len = 0;
    while(len < n && str[len] != ' ' && str[len] != '\t') {
        len++;
    }
    if (len >= n) {
        // Error
        return false;
    }
    outname->str = str;
    outname->len = len;
    // Skip space
    auto end = str + n;
    str = str + len;
    while(str < end && (*str == ' ' || *str == '\t')) {
        str++;
    }
    if (str == end) {
        // Error (no keys present)
        return false;
    }
    outkeys->str = str;
    outkeys->len = end - str;
    return true;
}

void MetadataStorage::insert_new_names(std::vector<MetadataStorage::SeriesT> items) {
    if (items.size() == 0) {
        return;
    }

    execute_query("BEGIN TRANSACTION;");

    // Write all data
    while(!items.empty()) {
        const size_t batchsize = 100;
        const size_t newsize = items.size() > batchsize ? items.size() - batchsize : 0;
        std::vector<MetadataStorage::SeriesT> batch(items.begin() + newsize, items.end());
        items.resize(newsize);
        std::stringstream query;
        query << "INSERT INTO akumuli_series (series_id, keyslist, storage_id)" << std::endl;
        bool first = true;
        for (auto item: batch) {
            LightweightString name, keys;
            auto stid = std::get<2>(item);
            if (split_series(std::get<0>(item), std::get<1>(item), &name, &keys)) {
                if (first) {
                    query << "\tSELECT '" << name << "' as series_id, '"
                                          << keys << "' as keyslist, "
                                          << stid << "  as storage_id"
                                          << std::endl;
                    first = false;
                } else {
                    query << "\tUNION "
                          <<   "SELECT '" << name << "', '"
                                          << keys << "', "
                                          << stid
                                          << std::endl;
                }
            }
        }
        std::string full_query = query.str();
        execute_query(full_query);
    }

    execute_query("END TRANSACTION;");
}

uint64_t MetadataStorage::get_prev_largest_id() {
    auto query = "SELECT max(storage_id) FROM akumuli_series;";
    try {
        auto results = select_query(query);
        auto row = results.at(0);
        if (row.empty()) {
            AKU_PANIC("Can't get max storage id");
        }
        auto id = row.at(0);
        if (id == "") {
            // Table is empty
            return 1ul;
        }
        return boost::lexical_cast<uint64_t>(id);
    } catch(...) {
        (*logger_)(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        AKU_PANIC("Can't get max storage id");
    }
}


aku_Status MetadataStorage::load_matcher_data(SeriesMatcher& matcher) {
    auto query = "SELECT series_id || ' ' || keyslist, storage_id FROM akumuli_series;";
    try {
        auto results = select_query(query);
        for(auto row: results) {
            if (row.size() != 2) {
                continue;
            }
            auto series = row.at(0);
            auto id = boost::lexical_cast<uint64_t>(row.at(1));
            matcher._add(series, id);
        }
    } catch(...) {
        (*logger_)(AKU_LOG_ERROR, boost::current_exception_diagnostic_information().c_str());
        return AKU_EGENERAL;
    }
    return AKU_SUCCESS;
}

//----------------------------------Volume----------------------------------------------

// TODO: remove max_cache_size
Volume::Volume(const char* file_name,
               aku_Config const& conf,
               bool enable_huge_tlb,
               aku_logger_cb_t logger)
    : mmap_(file_name, enable_huge_tlb, logger)
    , window_(conf.window_size)
    , max_cache_size_(conf.max_cache_size)
    , file_path_(file_name)
    , config_(conf)
    , logger_(logger)
    , is_temporary_ {0}
    , huge_tlb_(enable_huge_tlb)
{
    mmap_.panic_if_bad();  // panic if can't mmap volume
    page_ = reinterpret_cast<PageHeader*>(mmap_.get_pointer());
    cache_.reset(new Sequencer(page_, conf));
}

Volume::~Volume() {
    if (is_temporary_.load()) {
        mmap_.delete_file();
    }
}

PageHeader* Volume::get_page() const {
    return page_;
}

std::shared_ptr<Volume> Volume::safe_realloc() {
    uint32_t page_id = page_->page_id;
    uint32_t open_count = page_->open_count;
    uint32_t close_count = page_->close_count;

    std::string new_file_name = file_path_;
                new_file_name += ".tmp";

    // this volume is temporary and should live until
    // somebody is reading its data
    mmap_.move_file(new_file_name.c_str());
    mmap_.panic_if_bad();
    is_temporary_.store(true);

    std::shared_ptr<Volume> newvol;
    auto status = create_page_file(file_path_.c_str(), page_id, logger_);
    if (status != AKU_SUCCESS) {
        (*logger_)(AKU_LOG_ERROR, "Failed to create new volume");
        // Try to restore previous state on disk
        mmap_.move_file(file_path_.c_str());
        mmap_.panic_if_bad();
        AKU_PANIC("can't create new page file (out of space?)");
    }

    newvol.reset(new Volume(file_path_.c_str(), config_, huge_tlb_, logger_));
    newvol->page_->open_count = open_count;
    newvol->page_->close_count = close_count;
    return newvol;
}

void Volume::open() {
    page_->reuse();
    mmap_.flush();
}

void Volume::close() {
    page_->close();
    mmap_.flush();
}

void Volume::flush() {
    mmap_.flush();
    page_->checkpoint = page_->sync_count;
    mmap_.flush(0, sizeof(PageHeader));
}

void Volume::search(Caller& caller, InternalCursor* cursor, SearchQuery query) const {
    page_->search(caller, cursor, query);
}

//----------------------------------Storage---------------------------------------------

struct VolumeIterator {
    uint32_t                 compression_threshold;
    uint32_t                 max_cache_size;
    uint64_t                 window_size;
    std::vector<std::string> volume_names;
    aku_Status               error_code;

    VolumeIterator(std::shared_ptr<MetadataStorage> db, aku_logger_cb_t logger)
        : error_code(AKU_SUCCESS)
    {
        // 1. Read configuration data
        std::string creation_time;
        try {
            db->get_configs(&compression_threshold, &max_cache_size, &window_size, &creation_time);
        } catch(std::exception const& err) {
            (*logger)(AKU_LOG_ERROR, err.what());
            error_code = AKU_ENO_DATA;
            return;
        }

        // 2. Read volumes
        std::vector<MetadataStorage::VolumeDesc> volumes;
        try {
            volumes = db->get_volumes();
            if (volumes.size() == 0u) {
                throw std::runtime_error("no volumes specified");
            }
        } catch(std::exception const& err) {
            (*logger)(AKU_LOG_ERROR, err.what());
            error_code = AKU_ENO_DATA;
            return;
        }

        volume_names.resize(volumes.size());
        for(auto desc: volumes) {
            auto volume_index = desc.first;
            auto volume_path = desc.second;
            volume_names.at(volume_index) = volume_path;
        }

        // check result
        for(std::string const& path: volume_names) {
            if (path.empty()) {
                error_code = AKU_EBAD_ARG;
                (*logger)(AKU_LOG_ERROR, "invalid storage, one of the volumes is missing");
                return;
            }
        }
    }

    bool is_bad() const {
        return error_code != AKU_SUCCESS;
    }
};


Storage::Storage(const char* path, aku_FineTuneParams const& params)
    : compression(true)
    , open_error_code_(AKU_SUCCESS)
    , logger_(params.logger)
    , durability_(params.durability)
    , huge_tlb_(params.enable_huge_tlb != 0)
{
    // 0. Check that file exists
    auto filedesc = std::fopen(const_cast<char*>(path), "r");
    if (filedesc == nullptr) {
        // No such file
        open_error_code_ = AKU_ENOT_FOUND;
        (*logger_)(AKU_LOG_ERROR, "invalid path, no such file");
        return;
    }
    std::fclose(filedesc);

    // 1. Open db
    try {
        metadata_ = std::make_shared<MetadataStorage>(path, logger_);
    } catch(std::exception const& err) {
        (*logger_)(AKU_LOG_ERROR, err.what());
        open_error_code_ = AKU_ENOT_FOUND;
        return;
    }

    VolumeIterator v_iter(metadata_, logger_);

    if (v_iter.is_bad()) {
        open_error_code_ = v_iter.error_code;
        return;
    }

    config_.compression_threshold = v_iter.compression_threshold;
    // TODO: convert conf.max_cache_size from bytes
    config_.max_cache_size = v_iter.max_cache_size;
    config_.window_size = v_iter.window_size;
    ttl_ = v_iter.window_size;

    // create volumes list
    for(auto path: v_iter.volume_names) {
        PVolume vol;
        vol.reset(new Volume(path.c_str(), config_, huge_tlb_, logger_));
        volumes_.push_back(vol);
    }

    select_active_page();

    prepopulate_cache(config_.max_cache_size);
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
    if (active_page_->sync_count != active_page_->checkpoint) {
        active_page_->sync_count = active_page_->checkpoint;
        active_volume_->flush();
    }

    // Read data from sqlite to series matcher
    uint64_t nextid = 1 + metadata_->get_prev_largest_id();
    matcher_ = std::make_shared<SeriesMatcher>(nextid + 1);
    aku_Status status = metadata_->load_matcher_data(*matcher_);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can't read series names from sqlite");
    }
}

aku_Status Storage::get_open_error() const {
    return open_error_code_;
}

void Storage::advance_volume_(int local_rev) {
    if (local_rev == active_volume_index_.load()) {
        log_message("advance volume, current:");
        log_message("....page ID", active_volume_->page_->page_id);
        log_message("....close count", active_volume_->page_->close_count);
        log_message("....open count", active_volume_->page_->open_count);

        auto old_page_id = active_page_->page_id;
        AKU_UNUSED(old_page_id);

        int close_lock = active_volume_->cache_->reset();
        if (close_lock % 2 == 1) {
            active_volume_->cache_->merge_and_compress(active_page_);
        }
        active_volume_->close();
        log_message("page complete");

        // select next page in round robin order
        active_volume_index_++;
        auto last_volume = volumes_[active_volume_index_ % volumes_.size()];
        volumes_[active_volume_index_ % volumes_.size()] = last_volume->safe_realloc();
        active_volume_ = volumes_[active_volume_index_ % volumes_.size()];
        active_volume_->open();
        active_page_ = active_volume_->page_;

        auto new_page_id = active_page_->page_id;
        AKU_UNUSED(new_page_id);
        assert(new_page_id != old_page_id);

        log_message("next volume opened");
        log_message("....page ID", active_volume_->page_->page_id);
        log_message("....close count", active_volume_->page_->close_count);
        log_message("....open count", active_volume_->page_->open_count);
    }
    // Or other thread already done all the switching
    // just redo all the things
}

void Storage::log_message(const char* message) {
    (*logger_)(AKU_LOG_INFO, message);
}

void Storage::log_error(const char* message) {
    (*logger_)(AKU_LOG_ERROR, message);
}

void Storage::log_message(const char* message, uint64_t value) {
    using namespace std;
    stringstream fmt;
    fmt << message << ", " << value;
    (*logger_)(AKU_LOG_INFO, fmt.str().c_str());
}

// Reading

void Storage::search(Caller &caller, InternalCursor *cur, const SearchQuery &query) const {
    using namespace std;
    // Find pages
    // at this stage of development - simply get all pages :)
    vector<unique_ptr<ExternalCursor>> cursors;
    for(auto vol: volumes_) {
        // Search cache (optional, only for active page)
        if (vol == this->active_volume_) {
            aku_TimeStamp window;
            int seq_id;
            tie(window, seq_id) = active_volume_->cache_->get_window();
            if (query.direction == AKU_CURSOR_DIR_BACKWARD &&              // Cache searched only if cursor
               (query.lowerbound > window || query.upperbound > window))    // direction is backward.
            {
                auto ccur = CoroCursor::make(&Sequencer::search,            // Cache has optimistic concurrency
                                             active_volume_->cache_.get(),  // control and can easily return
                                             query, seq_id);                // AKU_EBUSY, because of that it
                cursors.push_back(move(ccur));                              // must be searched in a first place.
            }
        }
        // Search pages
        auto pcur = CoroCursor::make(&Volume::search, vol, query);
        cursors.push_back(move(pcur));
    }

    vector<ExternalCursor*> pcursors;
    transform( cursors.begin(), cursors.end()
             , back_inserter(pcursors)
             , [](unique_ptr<ExternalCursor>& v) { return v.get(); });

    assert(pcursors.size());
    StacklessFanInCursorCombinator fan_in_cursor(&pcursors[0], pcursors.size(), query.direction);

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
            cur->put(caller, results[i]);
        }
    }
    fan_in_cursor.close();
    cur->complete(caller);
}

void Storage::get_stats(aku_StorageStats* rcv_stats) {
    uint64_t used_space = 0,
             free_space = 0,
              n_entries = 0;

    for (PVolume const& vol: volumes_) {
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

aku_Status Storage::_write_impl(TimeSeriesValue &ts_value, aku_MemRange data) {
    while (true) {
        int local_rev = active_volume_index_.load();
        auto space_required = active_volume_->cache_->get_space_estimate();
        int status = AKU_SUCCESS;
        if (ts_value.is_blob()) {
            status = active_page_->add_chunk(data, space_required);
            ts_value.payload.blob.value = active_page_->last_offset;
        }
        switch (status) {
            case AKU_SUCCESS: {
                int merge_lock = 0;
                std::tie(status, merge_lock) = active_volume_->cache_->add(ts_value);
                if (merge_lock % 2 == 1) {

                    // Slow path //

                    // Update metadata store
                    std::vector<SeriesMatcher::SeriesNameT> names;
                    matcher_->pull_new_names(&names);
                    if (!names.empty()) {
                        metadata_->insert_new_names(names);
                    }

                    // Move data from cache to disk
                    status = active_volume_->cache_->merge_and_compress(active_volume_->get_page());
                    if (status == AKU_SUCCESS) {
                        switch(durability_) {
                        case AKU_MAX_DURABILITY:
                            // Max durability
                            active_volume_->flush();
                            break;
                        case AKU_DURABILITY_SPEED_TRADEOFF:
                            // Compromice some durability for speed
                            if ((merge_lock % 8) == 1) {
                                active_volume_->flush();
                            }
                            break;
                        case AKU_MAX_WRITE_SPEED:
                            // Max speed
                            if ((merge_lock % 32) == 1) {
                                active_volume_->flush();
                            }
                            break;
                        };
                    }
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
        }
    }
}

//! write binary data
aku_Status Storage::write_blob(aku_ParamId param, aku_TimeStamp ts, aku_MemRange data) {
    TimeSeriesValue ts_value(ts, param, active_page_->last_offset, data.length);
    return _write_impl(ts_value, data);
}

//! write binary data
aku_Status Storage::write_double(aku_ParamId param, aku_TimeStamp ts, double value) {
    aku_MemRange m = {};
    TimeSeriesValue ts_value(ts, param, value);
    return _write_impl(ts_value, m);
}

aku_Status Storage::write_double(const char* begin, const char* end, aku_TimeStamp ts, double value) {
    aku_ParamId id;
    auto status = _series_to_param_id(begin, end, &id);
    if (status == AKU_SUCCESS) {
        aku_MemRange m = {};
        TimeSeriesValue ts_value(ts, id, value);
        status = _write_impl(ts_value, m);
    }
    return status;
}

aku_Status Storage::_series_to_param_id(const char* begin, const char* end, uint64_t *value) {
    char buffer[AKU_LIMITS_MAX_SNAME];
    const char* keystr_begin = nullptr;
    const char* keystr_end = nullptr;
    auto status = SeriesParser::to_normal_form(begin, end,
                                               buffer, buffer+AKU_LIMITS_MAX_SNAME,
                                               &keystr_begin, &keystr_end);
    if (status == AKU_SUCCESS) {
        auto id = matcher_->match(buffer, keystr_end);
        if (id == 0) {
            *value = matcher_->add(buffer, keystr_end);
        } else {
            *value = id;
        }
    }
    return status;
}


// Standalone functions //


/** This function creates file with specified size
  */
static apr_status_t create_file(const char* file_name, uint64_t size, aku_logger_cb_t logger) {
    using namespace std;
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
        stringstream err;
        err << "Can't create file, error " << error_message << " on step " << success_count;
        (*logger)(AKU_LOG_ERROR, err.str().c_str());
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
static apr_status_t create_page_file(const char* file_name, uint32_t page_index, aku_logger_cb_t logger) {
    using namespace std;
    apr_status_t status;
    int64_t size = AKU_MAX_PAGE_SIZE;

    status = create_file(file_name, size, logger);
    if (status != APR_SUCCESS) {
        stringstream err;
        err << "Can't create page file " << file_name;
        (*logger)(AKU_LOG_ERROR, err.str().c_str());
        return status;
    }

    MemoryMappedFile mfile(file_name, false, logger);
    if (mfile.is_bad())
        return mfile.status_code();

    // Create index page
    auto index_ptr = mfile.get_pointer();
    auto index_page = new (index_ptr) PageHeader(0, AKU_MAX_PAGE_SIZE, page_index);

    // Activate the first page
    if (page_index == 0) {
        index_page->reuse();
    }
    return status;
}

/** Create page files, return list of statuses.
  */
static std::vector<apr_status_t> create_page_files(std::vector<std::string> const& targets, aku_logger_cb_t logger) {
    std::vector<apr_status_t> results(targets.size(), APR_SUCCESS);
    for (size_t ix = 0; ix < targets.size(); ix++) {
        apr_status_t res = create_page_file(targets[ix].c_str(), ix, logger);
        results[ix] = res;
    }
    return results;
}

static std::vector<apr_status_t> delete_files(const std::vector<std::string>& targets,
                                              const std::vector<apr_status_t>& statuses,
                                              aku_logger_cb_t logger)
{
    using namespace std;
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
                stringstream fmt;
                fmt << "Removing " << target;
                (*logger)(AKU_LOG_ERROR, fmt.str().c_str());
                status = apr_file_remove(target.c_str(), mem_pool);
                results.push_back(status);
                if (status != APR_SUCCESS) {
                    char error_message[1024];
                    apr_strerror(status, error_message, 1024);
                    stringstream err;
                    err << "Error [" << error_message << "] while deleting a file " << target;
                    (*logger)(AKU_LOG_ERROR, err.str().c_str());
                }
            }
            else {
                stringstream fmt;
                fmt << "Target " << target << " doesn't need to be removed";
                (*logger)(AKU_LOG_ERROR, fmt.str().c_str());
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
  * @return APR_EINIT on DB error.
  */
static apr_status_t create_metadata_page( const char* file_name
                                        , std::vector<std::string> const& page_file_names
                                        , uint32_t compression_threshold
                                        , uint64_t window_size
                                        , uint32_t max_cache_size
                                        , aku_logger_cb_t logger)
{
    using namespace std;
    try {
        auto storage = std::make_shared<MetadataStorage>(file_name, logger);

        auto now = apr_time_now();
        char date_time[0x100];
        apr_rfc822_date(date_time, now);

        storage->init_config(compression_threshold,
                             max_cache_size,
                             window_size,
                             date_time);

        std::vector<MetadataStorage::VolumeDesc> desc;
        int ix = 0;
        for(auto str: page_file_names) {
            desc.push_back(std::make_pair(ix++, str));
        }
        storage->init_volumes(desc);

    } catch (std::exception const& err) {
        std::stringstream fmt;
        fmt << "Can't create metadata file " << file_name << ", the error is: " << err.what();
        (*logger)(AKU_LOG_ERROR, fmt.str().c_str());
        return APR_EGENERAL;
    }
    return APR_SUCCESS;
}


apr_status_t Storage::new_storage(const char  *file_name,
                                  const char  *metadata_path,
                                  const char  *volumes_path,
                                  int          num_pages,
                                  uint32_t     compression_threshold,
                                  uint64_t     window_size,
                                  uint32_t     max_cache_size,
                                  aku_logger_cb_t logger)
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
            std::stringstream err;
            err << "Invalid volumes path: " << error_message;
            (*logger)(AKU_LOG_ERROR, err.str().c_str());
            apr_pool_destroy(mempool);
            AKU_APR_PANIC(status, error_message.c_str());
        }
        page_names.push_back(path);
    }

    apr_pool_clear(mempool);

    status = apr_dir_make(metadata_path, APR_OS_DEFAULT, mempool);
    if (status == APR_EEXIST) {
        (*logger)(AKU_LOG_INFO, "Metadata dir already exists");
    }
    status = apr_dir_make(volumes_path, APR_OS_DEFAULT, mempool);
    if (status == APR_EEXIST) {
        (*logger)(AKU_LOG_INFO, "Volumes dir already exists");
    }

    std::vector<apr_status_t> page_creation_statuses = create_page_files(page_names, logger);
    for(auto creation_status: page_creation_statuses) {
        if (creation_status != APR_SUCCESS) {
            (*logger)(AKU_LOG_ERROR, "Not all pages successfullly created. Cleaning up.");
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
        std::stringstream err;
        err << "Invalid metadata path: %s" << error_message;
        (*logger)(AKU_LOG_ERROR, err.str().c_str());
        apr_pool_destroy(mempool);
        AKU_APR_PANIC(status, error_message.c_str());
    }
    status = create_metadata_page(path, page_names, compression_threshold, window_size, max_cache_size, logger);
    apr_pool_destroy(mempool);
    return status;
}


apr_status_t Storage::remove_storage(const char* file_name, aku_logger_cb_t logger) {
    std::shared_ptr<MetadataStorage> db;
    try {
        db = std::make_shared<MetadataStorage>(file_name, logger);
    } catch(std::runtime_error const& err) {
        (*logger)(AKU_LOG_ERROR, err.what());
        return APR_EBADPATH;
    }

    VolumeIterator v_iter(db, logger);

    if (v_iter.is_bad()) {
        return v_iter.error_code;
    }

    apr_pool_t* mempool;
    apr_status_t status = apr_pool_create(&mempool, NULL);

    if (status != APR_SUCCESS) {
        (*logger)(AKU_LOG_ERROR, "can't create memory pool");
        return status;
    }

    // create volumes list
    for(auto path: v_iter.volume_names) {
        status = apr_file_remove(path.c_str(), mempool);
        if (status != APR_SUCCESS) {
            std::stringstream fmt;
            fmt << "can't remove file " << path;
            (*logger)(AKU_LOG_ERROR, fmt.str().c_str());
        }
    }

    status = apr_file_remove(file_name, mempool);
    apr_pool_destroy(mempool);
    return status;
}

}
