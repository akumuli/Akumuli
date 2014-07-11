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

#include "akumuli.h"
#include "storage.h"

using namespace Akumuli;


static const char* g_error_messages[] = {
    "OK",
    "No data",
    "Not enough memory",
    "Device is busy",
    "Can't find result",
    "Bad argument",
    "Overflow",
    "Invalid data",
    "Error, no details available",
    "Late write",
    "Unknown error code"
};

const char* aku_error_message(int error_code) {
    if (error_code >= 0 && error_code < 10) {
        return g_error_messages[error_code];
    }
    return g_error_messages[10];
}


struct MatchPred {
    std::vector<aku_ParamId> params_;
    MatchPred(aku_ParamId* begin, uint32_t n)
        : params_(begin, begin + n)
    {
    }

    SearchQuery::ParamMatch operator () (aku_ParamId id) const {
        return std::binary_search(params_.begin(), params_.end(), id) ? SearchQuery::MATCH : SearchQuery::NO_MATCH;
    }
};


struct CursorImpl : aku_Cursor {
    std::unique_ptr<ExternalCursor> cursor_;
    CursorImpl(Storage& storage, SearchQuery query) {
        status = AKU_SUCCESS;
        cursor_ = CoroCursor::make(&Storage::search, &storage, query);
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
    DatabaseImpl(const char* path, const aku_Config& config)
        : storage_(path, config)
    {
    }

    CursorImpl* select(aku_SelectQuery* query) {
        uint32_t scan_dir;
        aku_TimeStamp begin, end;
        if (query->begin < query->end) {
            begin = query->begin;
            end = query->end;
            scan_dir = AKU_CURSOR_DIR_FORWARD;
        } else {
            end = query->begin;
            begin = query->end;
            scan_dir = AKU_CURSOR_DIR_BACKWARD;
        }
        MatchPred pred(query->params, query->n_params);
        SearchQuery search_query(pred, {begin}, {end}, scan_dir);
        auto pcur = new CursorImpl(storage_, search_query);
        return pcur;
    }

    void flush() {
        storage_.commit();
    }

    void add_sample(uint32_t param_id, int64_t long_timestamp, aku_MemRange value) {
        TimeStamp ts;
        ts.value = long_timestamp;
        auto entry = Entry2(param_id, ts, value);
        storage_.write(entry);
    }

};

apr_status_t create_database( const char* 	file_name
                            , const char* 	metadata_path
                            , const char* 	volumes_path
                            , int32_t       num_volumes
                            )
{
    return Storage::new_storage(file_name, metadata_path, volumes_path, num_volumes);
}

aku_Status aku_flush_database(aku_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->flush();
}

aku_Status aku_add_sample(aku_Database* db, aku_ParamId param_id, aku_TimeStamp long_timestamp, aku_MemRange value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->add_sample(param_id, long_timestamp, value);
}

aku_Database* aku_open_database(const char* path, aku_Config config)
{
    aku_Database* ptr = new DatabaseImpl(path, config);
    return static_cast<aku_Database*>(ptr);
}

void aku_close_database(aku_Database* db)
{
    delete db;
}

aku_SelectQuery* aku_make_select_query(aku_TimeStamp begin, aku_TimeStamp end, uint32_t n_params, aku_ParamId *params) {
    size_t s = sizeof(aku_SelectQuery) + n_params*sizeof(uint32_t);
    auto p = malloc(s);
    auto res = reinterpret_cast<aku_SelectQuery*>(p);
    res->begin = begin;
    res->end = end;
    res->n_params = n_params;
    memcpy(&res->params, params, n_params*sizeof(uint32_t));
    std::sort(res->params, res->params + n_params);
    return res;
}

void aku_destroy(void* any) {
    free(any);
}

aku_Cursor* aku_select(aku_Database *db, aku_SelectQuery* query) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    return dbi->select(query);
}

