/**
 * Copyright (c) 2014 Eugene Lazin <4lazin@gmail.com>
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
 */

#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include <boost/lockfree/queue.hpp>
#include <boost/thread/barrier.hpp>

#include "logger.h"
// akumuli-storage API
#include "akumuli.h"
#include "akumuli_config.h"

// TODO: rename file

namespace Akumuli {

//! Abstraction layer above aku_Cursor
struct DbCursor {
    virtual ~DbCursor() = default;
    //! Read data from cursor
    virtual size_t read(void* dest, size_t dest_size) = 0;

    //! Check is cursor is done reading
    virtual int is_done() = 0;

    //! Check for error condition
    virtual bool is_error(aku_Status* out_error_code_or_null) = 0;

    //! Close cursor
    virtual void close() = 0;
};


//! Database session, maps to aku_Session directly
struct DbSession {
    virtual ~DbSession() = default;

    //! Write value to DB
    virtual aku_Status write(const aku_Sample& sample) = 0;

    //! Execute database query
    virtual std::shared_ptr<DbCursor> query(std::string query) = 0;

    //! Execute suggest query
    virtual std::shared_ptr<DbCursor> suggest(std::string query) = 0;

    //! Execute search query
    virtual std::shared_ptr<DbCursor> search(std::string query) = 0;

    //! Convert paramid to series name
    virtual int param_id_to_series(aku_ParamId id, char* buffer, size_t buffer_size) = 0;

    virtual aku_Status series_to_param_id(const char* name, size_t size, aku_Sample* sample) = 0;

    virtual int name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) = 0;
};


//! Abstraction layer above aku_Database
struct DbConnection {

    virtual ~DbConnection() = default;

    virtual std::string get_all_stats() = 0;

    virtual std::shared_ptr<DbSession> create_session() = 0;
};


class AkumuliSession : public DbSession {
    aku_Session* session_;
public:
    AkumuliSession(aku_Session* session);
    virtual ~AkumuliSession() override;
    virtual aku_Status write(const aku_Sample &sample) override;
    virtual std::shared_ptr<DbCursor> query(std::string query) override;
    virtual std::shared_ptr<DbCursor> suggest(std::string query) override;
    virtual std::shared_ptr<DbCursor> search(std::string query) override;
    virtual int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) override;
    virtual aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) override;
    virtual int name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) override;
};


//! Object of this class writes everything to the database
class AkumuliConnection : public DbConnection {

    std::string   dbpath_;
    aku_Database* db_;

public:
    AkumuliConnection(const char* path);

    virtual ~AkumuliConnection() override;

    virtual std::string get_all_stats() override;

    virtual std::shared_ptr<DbSession> create_session() override;
};

}  // namespace Akumuli
