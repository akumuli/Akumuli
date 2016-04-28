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

#include <cstdio>
#include <cstdlib>
#include <string>
#include <memory>
#include <iostream>

#include <apr_dbd.h>

#include "akumuli.h"
#include "storage.h"
#include "datetime.h"
#include "log_iface.h"
#include "status_util.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using namespace Akumuli;

//! Pool for `apr_dbd_init`
static apr_pool_t* g_dbd_pool = nullptr;

void aku_console_logger(aku_LogLevel tag, const char* msg) {
    apr_time_t now = apr_time_now();
    char ts[APR_RFC822_DATE_LEN];
    if (apr_rfc822_date(ts, now) != APR_SUCCESS) {
        memset(ts, ' ', APR_RFC822_DATE_LEN);
        ts[sizeof(ts) - 1] = 0;
    }
    char tagstr[9];                    // I don't want to use manipulators on cerr here
    snprintf(tagstr, 9, "%08X", tag);  // because this can break formatting in host application.
    std::cerr << ts << " | " << tagstr << " | " << msg << std::endl;
}

void aku_initialize(aku_panic_handler_t optional_panic_handler, aku_logger_cb_t logger) {
    // initialize logger
    if (logger == nullptr) {
        logger = &aku_console_logger;
        aku_console_logger(AKU_LOG_ERROR, "Logger not set, console logger will be used");
    }
    Logger::set_logger(logger);
    // initialize libapr
    apr_initialize();
    // initialize aprdbd
    if (optional_panic_handler != nullptr) {
        set_panic_handler(optional_panic_handler);
    }
    auto status = apr_pool_create(&g_dbd_pool, nullptr);
    if (status != APR_SUCCESS) {
        AKU_PANIC("Initialization error");
    }
    status = apr_dbd_init(g_dbd_pool);
    if (status != APR_SUCCESS) {
        AKU_PANIC("DBD initialization error");
    }
}

const char* aku_error_message(int error_code) {
    return StatusUtil::c_str((aku_Status)error_code);
}


struct CursorImpl : aku_Cursor {
    std::unique_ptr<ExternalCursor> cursor_;
    aku_Status status_;
    std::string query_;

    CursorImpl(Storage& storage, const char* query)
        : query_(query)
    {
        status_ = AKU_SUCCESS;
        cursor_ = CoroCursor::make(&Storage::search, &storage, query_.data());
    }

    ~CursorImpl() {
        cursor_->close();
    }

    bool is_done() const {
        return cursor_->is_done();
    }

    bool is_error(aku_Status* out_error_code_or_null) const {
        if (status_ != AKU_SUCCESS) {
            *out_error_code_or_null = status_;
            return false;
        }
        return cursor_->is_error(out_error_code_or_null);
    }

    size_t read_values( void  *values
                      , size_t values_size )
    {
        return cursor_->read_ex(values, values_size);
    }
};


/** 
 * Object that extends a Database struct.
 * Can be used from "C" code.
 */
struct DatabaseImpl : public aku_Database
{
    Storage storage_;

    // private fields
    DatabaseImpl(const char* path, const aku_FineTuneParams& config)
        : storage_(path, config)
    {
    }

    void debug_print() const {
        storage_.debug_print();
    }

    aku_Status series_to_param_id(const char* begin, const char* end, aku_Sample *out_sample) {
        return storage_.series_to_param_id(begin, end, &out_sample->paramid);
    }

    int param_id_to_series(aku_ParamId id, char* buffer, size_t size) const {
        return storage_.param_id_to_series(id, buffer, size);
    }

    aku_Status get_open_error() const {
        return storage_.get_open_error();
    }

    void close() {
        storage_.close();
    }

    CursorImpl* query(const char* query) {
        auto pcur = new CursorImpl(storage_, std::move(query));
        return pcur;
    }

    aku_Status add_double(aku_ParamId param_id, aku_Timestamp ts, double value) {
        return storage_.write_double(param_id, ts, value);
    }

    aku_Status add_sample(aku_Sample const* sample) {
        aku_Status status = add_double(sample->paramid, sample->timestamp, sample->payload.float64);
        return status;
    }

    // Stats
    void get_storage_stats(aku_StorageStats* recv_stats) {
        storage_.get_stats(recv_stats);
    }

    std::vector<Storage::PVolume> iter_volumes() const {
        return storage_.volumes_;
    }
};

apr_status_t aku_create_database( const char     *file_name
                                , const char     *metadata_path
                                , const char     *volumes_path
                                , int32_t         num_volumes
                                , aku_logger_cb_t logger)
{
    if (logger == nullptr) {
        logger = &aku_console_logger;
    }
    return Storage::new_storage(file_name, metadata_path, volumes_path, num_volumes, logger, false);
}

apr_status_t aku_create_database_ex( const char     *file_name
                                   , const char     *metadata_path
                                   , const char     *volumes_path
                                   , int32_t         num_volumes
                                   , uint64_t        page_size
                                   , aku_logger_cb_t logger)
{
    if (logger == nullptr) {
        logger = &aku_console_logger;
    }
    return Storage::new_storage(file_name, metadata_path, volumes_path, num_volumes, logger, page_size);
}

apr_status_t aku_remove_database(const char* file_name, aku_logger_cb_t logger) {

    return Storage::remove_storage(file_name, logger);
}

aku_Status aku_write_double_raw(aku_Database* db, aku_ParamId param_id, aku_Timestamp timestamp, double value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->add_double(param_id, timestamp, value);
}

aku_Status aku_write(aku_Database* db, const aku_Sample* sample) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->add_sample(sample);
}


aku_Status aku_parse_duration(const char* str, int* value) {
    try {
        *value = DateTimeUtil::parse_duration(str, strlen(str));
    } catch (...) {
        return AKU_EBAD_ARG;
    }
    return AKU_SUCCESS;
}

aku_Status aku_parse_timestamp(const char* iso_str, aku_Sample* sample) {
    try {
        sample->timestamp = DateTimeUtil::from_iso_string(iso_str);
    } catch (...) {
        return AKU_EBAD_ARG;
    }
    return AKU_SUCCESS;
}

aku_Status aku_series_to_param_id(aku_Database* db, const char* begin, const char* end, aku_Sample* sample) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->series_to_param_id(begin, end, sample);
}

aku_Database* aku_open_database(const char* path, aku_FineTuneParams config)
{
    if (config.logger == nullptr) {
        // Use default console logger if user doesn't set it
        config.logger = &aku_console_logger;
    }
    if (config.durability != AKU_MAX_DURABILITY &&
        config.durability != AKU_DURABILITY_SPEED_TRADEOFF &&
        config.durability != AKU_MAX_WRITE_SPEED)
    {
        config.durability = AKU_MAX_DURABILITY;
        (*config.logger)(AKU_LOG_INFO, "config.durability = default(AKU_MAX_DURABILITY)");
    }
    if (config.compression_threshold == 0) {
        config.compression_threshold = AKU_DEFAULT_COMPRESSION_THRESHOLD;
        (*config.logger)(AKU_LOG_INFO, "config.compression_threshold = default(AKU_DEFAULT_COMPRESSION_THRESHOLD)");
    }
    if (config.window_size == 0) {
        config.window_size = AKU_DEFAULT_WINDOW_SIZE;
        (*config.logger)(AKU_LOG_INFO, "config.window_size = default(AKU_DEFAULT_WINDOW_SIZE)");
    }
    if (config.max_cache_size == 0) {
        config.max_cache_size = AKU_DEFAULT_MAX_CACHE_SIZE;
        (*config.logger)(AKU_LOG_INFO, "config.window_size = default(AKU_DEFAULT_WINDOW_SIZE)");
    }
    auto ptr = new DatabaseImpl(path, config);
    return static_cast<aku_Database*>(ptr);
}

aku_Status aku_open_status(aku_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->get_open_error();
}

void aku_close_database(aku_Database* db)
{
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->close();
    delete dbi;
}

aku_SelectQuery* aku_make_select_query(aku_Timestamp begin, aku_Timestamp end, uint32_t n_params, aku_ParamId *params) {
    size_t s = sizeof(aku_SelectQuery) + n_params*sizeof(aku_ParamId);
    auto p = malloc(s);
    auto res = reinterpret_cast<aku_SelectQuery*>(p);
    res->begin = begin;
    res->end = end;
    res->n_params = n_params;
    memcpy(&res->params, params, n_params*sizeof(aku_ParamId));
    std::sort(res->params, res->params + n_params);
    return res;
}

void aku_destroy(void* any) {
    free(any);
}

aku_Cursor* aku_query(aku_Database* db, const char* query) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->query(query);
}

void aku_cursor_close(aku_Cursor* pcursor) {
    CursorImpl* pimpl = reinterpret_cast<CursorImpl*>(pcursor);
    delete pimpl;
}

size_t aku_cursor_read( aku_Cursor       *cursor
                      , void             *dest
                      , size_t            dest_size)
{
    // read columns from data store
    CursorImpl* pimpl = reinterpret_cast<CursorImpl*>(cursor);
    return pimpl->read_values(dest, dest_size);
}

int aku_cursor_is_done(aku_Cursor* pcursor) {
    CursorImpl* pimpl = reinterpret_cast<CursorImpl*>(pcursor);
    return static_cast<int>(pimpl->is_done());
}

int aku_cursor_is_error(aku_Cursor* pcursor, aku_Status* out_error_code_or_null) {
    CursorImpl* pimpl = reinterpret_cast<CursorImpl*>(pcursor);
    return static_cast<int>(pimpl->is_error(out_error_code_or_null));
}

int aku_timestamp_to_string(aku_Timestamp ts, char* buffer, size_t buffer_size) {
    return DateTimeUtil::to_iso_string(ts, buffer, buffer_size);
}

int aku_param_id_to_series(aku_Database* db, aku_ParamId id, char* buffer, size_t buffer_size) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->param_id_to_series(id, buffer, buffer_size);
}

//--------------------------------
//         Statistics
//--------------------------------

void aku_global_search_stats(aku_SearchStats* rcv_stats, int reset) {
    PageHeader::get_search_stats(rcv_stats, reset);
}

void aku_global_storage_stats(aku_Database *db, aku_StorageStats* rcv_stats) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->get_storage_stats(rcv_stats);
}

int aku_json_stats(aku_Database *db, char* buffer, size_t size) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    try {
        boost::property_tree::ptree ptree;

        // Get search stats
        aku_SearchStats sstats;
        PageHeader::get_search_stats(&sstats, false);

        // Binary-search
        ptree.put("search_stats.binary_search.steps", sstats.bstats.n_steps);
        ptree.put("search_stats.binary_search.times", sstats.bstats.n_times);
        // Scan
        ptree.put("search_stats.scan.bytes_read_backward", sstats.scan.bwd_bytes);
        ptree.put("search_stats.scan.bytes_read_forward", sstats.scan.fwd_bytes);
        // Interpolation search
        ptree.put("search_stats.interpolation_search.matches", sstats.istats.n_matches);
        ptree.put("search_stats.interpolation_search.overshoots", sstats.istats.n_overshoots);
        ptree.put("search_stats.interpolation_search.undershoots", sstats.istats.n_undershoots);
        ptree.put("search_stats.interpolation_search.pages_in_core_found", sstats.istats.n_pages_in_core_found);
        ptree.put("search_stats.interpolation_search.pages_in_core_miss", sstats.istats.n_pages_in_core_miss);
        ptree.put("search_stats.interpolation_search.page_in_core_checks", sstats.istats.n_page_in_core_checks);
        ptree.put("search_stats.interpolation_search.page_in_core_errors", sstats.istats.n_page_in_core_errors);
        ptree.put("search_stats.interpolation_search.reduced_to_one_page", sstats.istats.n_reduced_to_one_page);
        ptree.put("search_stats.interpolation_search.steps", sstats.istats.n_steps);
        ptree.put("search_stats.interpolation_search.times", sstats.istats.n_times);

        // Get per-volume stats
        auto volumes = dbi->iter_volumes();
        int iter = 0;
        for (const auto volume: volumes) {
            std::stringstream fmt;
            fmt << "volume_" << iter << ".";
            auto path = fmt.str();
            auto file = volume->file_path_;
            ptree.put(path + "path", file);
            const PageHeader* page = volume->page_;
            ptree.put(path + "close_count", page->get_close_count());
            ptree.put(path + "entries_count", page->get_entries_count());
            ptree.put(path + "free_space", page->get_free_space());
            ptree.put(path + "open_count", page->get_open_count());
            ptree.put(path + "num_pages", page->get_numpages());
            ptree.put(path + "page_id", page->get_page_id());
            iter++;
        }


        // encode json
        std::stringstream out;
        boost::property_tree::json_parser::write_json(out, ptree, true);
        auto str = out.str();
        if (str.size() > size) {
            return -1*(int)str.size();
        }
        strcpy(buffer, str.c_str());
        return (int)str.size();
    } catch (std::exception const& e) {
        (*dbi->storage_.logger_)(AKU_LOG_ERROR, e.what());
    } catch (...) {
        AKU_PANIC("unexpected error in `aku_json_stats`");
    }
    return -1;
}

void aku_debug_print(aku_Database *db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->debug_print();
}

