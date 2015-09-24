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

using namespace Akumuli;

//! Pool for `apr_dbd_init`
static apr_pool_t* g_dbd_pool = nullptr;

void aku_initialize(aku_panic_handler_t optional_panic_handler) {
    apr_initialize();
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

static const char* g_error_messages[] = {
    "OK",
    "no data",
    "not enough memory",
    "device is busy",
    "not found",
    "bad argument",
    "overflow",
    "invalid data",
    "unknown error",
    "late write",
    "not implemented",
    "query parsing error",
    "anomaly detector can't work with negative values",
    "unknown error code"
};

const char* aku_error_message(int error_code) {
    if (error_code >= 0 && error_code < AKU_EMAX_ERROR) {
        return g_error_messages[error_code];
    }
    return g_error_messages[AKU_EMAX_ERROR];
}

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

    size_t read_values( aku_Sample     *values
                      , size_t          values_size )
    {
        return cursor_->read(values, values_size);
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

    // TODO: remove obsolete
    CursorImpl* select(aku_SelectQuery const* query) {
        throw "depricated";
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
    return Storage::new_storage(file_name, metadata_path, volumes_path, num_volumes, logger);
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

aku_Cursor* aku_select(aku_Database *db, const aku_SelectQuery* query) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->select(query);
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
                      , aku_Sample       *dest
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

void aku_debug_print(aku_Database *db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->debug_print();
}
