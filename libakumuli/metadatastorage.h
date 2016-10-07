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
 */

#pragma once
#include <cstddef>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>

#include <apr.h>
#include <apr_dbd.h>

#include "akumuli_def.h"
#include "seriesparser.h"

namespace Akumuli {

//! Delete apr pool
void delete_apr_pool(apr_pool_t* p);

//! APR DBD handle deleter
struct AprHandleDeleter {
    const apr_dbd_driver_t* driver;
    AprHandleDeleter(const apr_dbd_driver_t* driver);
    void operator()(apr_dbd_t* handle);
};


/** Sqlite3 backed storage for metadata.
  * Metadata includes:
  * - Volumes list
  * - Conviguration data
  * - Key to id mapping
  */
struct MetadataStorage {
    // Typedefs
    typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)> PoolT;
    typedef const apr_dbd_driver_t* DriverT;
    typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter> HandleT;
    typedef std::pair<int, std::string>                  VolumeDesc;
    typedef apr_dbd_prepared_t* PreparedT;
    typedef SeriesMatcher::SeriesNameT SeriesT;

    // Members
    PoolT           pool_;
    DriverT         driver_;
    HandleT         handle_;
    PreparedT       insert_;

    // Synchronization
    std::mutex sync_lock_;
    std::condition_variable sync_cvar_;
    std::unordered_map<aku_ParamId, std::vector<u64>> pending_rescue_points_;

    /** Create new or open existing db.
      * @throw std::runtime_error in a case of error
      */
    MetadataStorage(const char* db);


    // Creation //

    /** Create tables if database is empty
      * @throw std::runtime_error in a case of error
      */
    void create_tables();

    /** Initialize volumes table
      * @throw std::runtime_error in a case of error
      */
    void init_volumes(std::vector<VolumeDesc> volumes);

    void init_config(const char* creation_datetime);

    // Retreival //

    /** Read list of volumes and their sequence numbers.
      * @throw std::runtime_error in a case of error
      */
    std::vector<VolumeDesc> get_volumes() const;

    void get_configs(std::string* creation_datetime);

    /** Read larges series id */
    u64 get_prev_largest_id();

    aku_Status load_matcher_data(SeriesMatcher& matcher);

    aku_Status load_rescue_points(std::unordered_map<u64, std::vector<u64>>& mapping);

    // Synchronization

    void add_rescue_point(aku_ParamId id, std::vector<u64>&& val);

    aku_Status wait_for_sync_request(int timeout_us);

    void sync_with_metadata_storage(std::function<void(std::vector<SeriesT>*)> pull_new_names);

    //! Forces `wait_for_sync_request` to return immediately
    void force_sync();

    // should be private:

    void begin_transaction();

    void end_transaction();

    /** Add new series to the metadata storage (generate sql query and execute it).
      */
    void insert_new_names(std::vector<SeriesT>&& items);

    /** Insert or update rescue provided points (generate sql query and execute it).
      */
    void upsert_rescue_points(std::unordered_map<aku_ParamId, std::vector<u64> > &&input);

private:

    /** Execute query that doesn't return anything.
      * @throw std::runtime_error in a case of error
      * @return number of rows changed
      */
    int execute_query(std::string query);

    typedef std::vector<std::string> UntypedTuple;

    /** Execute select query and return untyped results.
      * @throw std::runtime_error in a case of error
      * @return bunch of strings with results
      */
    std::vector<UntypedTuple> select_query(const char* query) const;
};
}
