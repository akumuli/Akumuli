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

//#ifdef libakumuli2
#include "storage2.h"
//#else
//#include "storage.h"
//#endif

#include "datetime.h"
#include "log_iface.h"
#include "status_util.h"
#include "cursor.h"

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

    CursorImpl(std::shared_ptr<StorageSession> storage, const char* query)
        : query_(query)
    {
        status_ = AKU_SUCCESS;
        cursor_ = CoroCursor::make(&StorageSession::query, storage, query_.data());
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

    u32 read_values( void  *values
                   , u32    values_size )
    {
        return cursor_->read(values, values_size);
    }
};


class Session : public aku_Session {
    std::shared_ptr<StorageSession> session_;
public:

    Session(std::shared_ptr<StorageSession> session)
        : session_(session)
    {
    }

    aku_Status series_to_param_id(const char* begin, const char* end, aku_Sample *out_sample) {
        return session_->init_series_id(begin, end, out_sample);
    }

    int param_id_to_series(aku_ParamId id, char* buffer, size_t size) const {
        return session_->get_series_name(id, buffer, size);
    }

    aku_Status add_sample(aku_Sample const& sample) {
        return session_->write(sample);
    }
};

/** 
 * Object that extends a Database struct.
 * Can be used from "C" code.
 */
class DatabaseImpl : public aku_Database
{
    std::shared_ptr<Storage> storage_;
public:
    // private fields
    DatabaseImpl(const char* path)
        : storage_(std::make_shared<Storage>(path))
    {
    }

    void close() {
        storage_->close();
    }

    static aku_Database* create(const char* path) {
        DatabaseImpl* ptr = new DatabaseImpl(path);
        return static_cast<aku_Database*>(ptr);
    }

    static void free(aku_Database* ptr) {
        DatabaseImpl* pimpl = reinterpret_cast<DatabaseImpl*>(ptr);
        pimpl->close();
        delete pimpl;
    }

    static void free(aku_Session* ptr) {
        auto pimpl = reinterpret_cast<Session*>(ptr);
        delete pimpl;
    }

    void debug_print() const {
        storage_->debug_print();
    }

    aku_Session* create_session() {
        auto disp = storage_->create_write_session();
        Session* ptr = new Session(disp);
        return static_cast<aku_Session*>(ptr);
    }

    CursorImpl* query(const char*) {
        AKU_PANIC("Not implemented");
    }

    // Stats
    void get_storage_stats(aku_StorageStats*) {
        AKU_PANIC("Not implemented");
    }
};

aku_Status aku_create_database_ex( const char     *file_name
                                 , const char     *metadata_path
                                 , const char     *volumes_path
                                 , i32             num_volumes
                                 , u64             page_size)
{
    return Storage::new_database(file_name, metadata_path, volumes_path, num_volumes, page_size);
}

aku_Status aku_create_database( const char     *file_name
                              , const char     *metadata_path
                              , const char     *volumes_path
                              , i32             num_volumes)
{
    static const u64 vol_size = 4096ul*1024*1024; // pages (4GB total)
    return aku_create_database_ex(file_name, metadata_path, volumes_path, num_volumes, vol_size);
}


aku_Status aku_remove_database(const char* file_name, bool force) {

    return Storage::remove_storage(file_name, force);
}

aku_Session* aku_create_session(aku_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->create_session();
}

void aku_destroy_session(aku_Session* session) {
    DatabaseImpl::free(session);
}

aku_Status aku_write_double_raw(aku_Session* session, aku_ParamId param_id, aku_Timestamp timestamp,  double value) {
    aku_Sample sample;
    sample.timestamp = timestamp;
    sample.paramid = param_id;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.payload.float64 = value;
    auto ises = reinterpret_cast<Session*>(session);
    return ises->add_sample(sample);
}

aku_Status aku_write(aku_Session* session, const aku_Sample* sample) {
    auto ises = reinterpret_cast<Session*>(session);
    return ises->add_sample(*sample);
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

aku_Status aku_series_to_param_id(aku_Session* session, const char* begin, const char* end, aku_Sample* sample) {
    auto ises = reinterpret_cast<Session*>(session);
    return ises->series_to_param_id(begin, end, sample);
}

aku_Database* aku_open_database(const char* path, aku_FineTuneParams parameters) {
    AKU_UNUSED(parameters);
    return DatabaseImpl::create(path);
}

void aku_close_database(aku_Database* db) {
    DatabaseImpl::free(db);
}

aku_Cursor* aku_query(aku_Database* db, const char* query) {
    AKU_PANIC("Not implemented");
}

void aku_cursor_close(aku_Cursor* pcursor) {
    AKU_PANIC("Not implemented");
}

size_t aku_cursor_read( aku_Cursor       *cursor
                      , void             *dest
                      , size_t            dest_size)
{
    AKU_PANIC("Not implemented");
}

int aku_cursor_is_done(aku_Cursor* pcursor) {
    AKU_PANIC("Not implemented");
}

int aku_cursor_is_error(aku_Cursor* pcursor, aku_Status* out_error_code_or_null) {
    AKU_PANIC("Not implemented");
}

int aku_timestamp_to_string(aku_Timestamp ts, char* buffer, size_t buffer_size) {
    return DateTimeUtil::to_iso_string(ts, buffer, buffer_size);
}

int aku_param_id_to_series(aku_Session* session, aku_ParamId id, char* buffer, size_t buffer_size) {
    auto ises = reinterpret_cast<Session*>(session);
    return ises->param_id_to_series(id, buffer, buffer_size);
}

//--------------------------------
//         Statistics
//--------------------------------

void aku_global_search_stats(aku_SearchStats* rcv_stats, int reset) {
    AKU_PANIC("Not implemented");
}

void aku_global_storage_stats(aku_Database *db, aku_StorageStats* rcv_stats) {
    AKU_PANIC("Not implemented");
}

int aku_json_stats(aku_Database *db, char* buffer, size_t size) {
    AKU_PANIC("Not implemented");
}

void aku_debug_print(aku_Database *db) {
    AKU_PANIC("Not implemented");
}

