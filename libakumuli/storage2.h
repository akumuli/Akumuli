/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "akumuli_def.h"
#include "metadatastorage.h"
#include "index/seriesparser.h"
#include "util.h"

#include "storage_engine/blockstore.h"
#include "storage_engine/nbtree.h"
#include "storage_engine/column_store.h"

#include "internal_cursor.h"

#include <boost/thread.hpp>

namespace Akumuli {

class Storage;

class StorageSession : public std::enable_shared_from_this<StorageSession> {
    std::shared_ptr<Storage> storage_;
    PlainSeriesMatcher local_matcher_;
    std::shared_ptr<StorageEngine::CStoreSession> session_;
    //! Temporary query matcher
    mutable std::shared_ptr<PlainSeriesMatcher> matcher_substitute_;
public:
    StorageSession(std::shared_ptr<Storage> storage, std::shared_ptr<StorageEngine::CStoreSession> session);

    aku_Status write(aku_Sample const& sample);

    /** Match series name. If series with such name doesn't exists - create it.
      * This method should be called for each sample to init its `paramid` field.
      */
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample);

    /** Match series name in joined form (foo:bar:buz tag=val will is a shorthand for foo tag=val, bar tag=val
     * and buz tag=val). Return array of series ids in order (foo, bar, buz) through ids array.
     * Return number of series or negative value on error (error code * -1).
     */
    int get_series_ids(const char* begin, const char* end, aku_ParamId* ids, size_t ids_size);

    int get_series_name(aku_ParamId id, char* buffer, size_t buffer_size);

    void query(InternalCursor* cur, const char* query) const;

    /**
     * @brief suggest query implementation
     * @param cur is a pointer to internal cursor
     * @param query is a string that contains query
     */
    void suggest(InternalCursor* cur, const char* query) const;

    /**
     * @brief search query implementation
     * @param cur is a pointer to internal cursor
     * @param query is a string that contains query
     */
    void search(InternalCursor* cur, const char* query) const;

    // Temporary reset series matcher
    void set_series_matcher(std::shared_ptr<PlainSeriesMatcher> matcher) const;
    void clear_series_matcher() const;
};

class Storage : public std::enable_shared_from_this<Storage> {
    std::shared_ptr<StorageEngine::BlockStore> bstore_;
    std::shared_ptr<StorageEngine::ColumnStore> cstore_;
    std::atomic<int> done_;
    boost::barrier close_barrier_;
    mutable std::mutex lock_;
    SeriesMatcher global_matcher_;
    std::shared_ptr<MetadataStorage> metadata_;

    void start_sync_worker();

    aku_Status parse_query(const boost::property_tree::ptree &ptree, QP::ReshapeRequest* req) const;
public:

    // Create empty in-memory storage
    Storage();

    // Open file-backed storage
    Storage(const char* path);

    /** C-tor for test */
    Storage(std::shared_ptr<MetadataStorage>            meta,
            std::shared_ptr<StorageEngine::BlockStore>  bstore,
            std::shared_ptr<StorageEngine::ColumnStore> cstore,
            bool                                        start_worker);

    //! Match series name. If series with such name doesn't exists - create it.
    aku_Status init_series_id(const char* begin, const char* end, aku_Sample *sample, PlainSeriesMatcher *local_matcher);

    int get_series_name(aku_ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher);

    //! Create new write session
    std::shared_ptr<StorageSession> create_write_session();

    void query(StorageSession const* session, InternalCursor* cur, const char* query) const;

    /**
     * @brief suggest query implementation
     * @param session is a session pointer
     * @param cur is an internal cursor
     * @param query is a query string (JSON)
     */
    void suggest(StorageSession const* session, InternalCursor* cur, const char* query) const;

    /**
     * @brief search query implementation
     * @param session is a session pointer
     * @param cur is an internal cursor
     * @param query is a query string (JSON)
     */
    void search(StorageSession const* session, InternalCursor* cur, const char* query) const;

    void debug_print() const;

    void _update_rescue_points(aku_ParamId id, std::vector<StorageEngine::LogicAddr>&& rpoints);

    /** This method should be called before object destructor.
      * All ingestion sessions should be stopped first.
      */
    void close();

    /** Create empty database from scratch.
      * @param base_file_name is database name (excl suffix)
      * @param metadata_path is a path to metadata storage
      * @param volumes_path is a path to volumes storage
      * @param num_volumes defines how many volumes should be crated
      * @param page_size is a size of the individual page in bytes
      * @return operation status
      */
    static aku_Status new_database( const char     *base_file_name
                                  , const char     *metadata_path
                                  , const char     *volumes_path
                                  , i32             num_volumes
                                  , u64             page_size
                                  , bool            allocate);

    /**
     * @brief Open storage and generate report (dont' modify anything)
     * @param path to sqlite3 database file
     * @return status
     */
    static aku_Status generate_report(const char* path, const char* output);

    static aku_Status generate_recovery_report(const char* path, const char* output);

    /** Remove existing database
      * @param file_name is a database name
      * @param force forces database deletion even if database is not empty
      * @return AKU_SUCCESS on success or AKU_ENOT_PERMITTED if database contains data and `force` is false or
      *         AKU_EACCESS if database there is not enough priveleges to delete the files
      */
    static aku_Status remove_storage(const char* file_name, bool force);

    boost::property_tree::ptree get_stats();
};

}
