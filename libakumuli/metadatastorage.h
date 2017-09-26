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
#include <boost/optional.hpp>

#include <apr.h>
#include <apr_dbd.h>

#include "akumuli_def.h"
#include "index/seriesparser.h"
#include "volumeregistry.h"

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
struct MetadataStorage : VolumeRegistry {
    // Typedefs
    typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)> PoolT;
    typedef const apr_dbd_driver_t* DriverT;
    typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter> HandleT;
    typedef apr_dbd_prepared_t* PreparedT;
    typedef PlainSeriesMatcher::SeriesNameT SeriesT;

    // Members
    PoolT           pool_;
    DriverT         driver_;
    HandleT         handle_;
    PreparedT       insert_;

    // Synchronization
    mutable std::mutex                                sync_lock_;
    std::condition_variable                           sync_cvar_;
    std::unordered_map<aku_ParamId, std::vector<u64>> pending_rescue_points_;
    std::unordered_map<u32, VolumeDesc>               pending_volumes_;

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

    void init_config(const char* db_name,
                     const char* creation_datetime,
                     const char* bstore_type);

    // Retreival //

    /** Read list of volumes and their sequence numbers.
      * @throw std::runtime_error in a case of error
      */
    virtual std::vector<VolumeDesc> get_volumes() const;

    /**
     * @brief Add NEW volume synchroniously
     * @param vol is a volume description
     */
    virtual void add_volume(const VolumeDesc& vol);

    /**
     * @brief Get value of the configuration parameter
     * @param param_name is a name of the configuration parameter
     * @param value is a pointer that should receive configuration value
     * @return true on succes, false otherwise
     */
    bool get_config_param(const std::string param_name, std::string* value);

    /** Read larges series id */
    boost::optional<u64> get_prev_largest_id();

    aku_Status load_matcher_data(SeriesMatcherBase &matcher);

    aku_Status load_rescue_points(std::unordered_map<u64, std::vector<u64>>& mapping);

    // Synchronization

    void add_rescue_point(aku_ParamId id, std::vector<u64>&& val);

    /**
     * @brief Add/update volume metadata asynchronously
     * @param vol is a volume description
     */
    virtual void update_volume(const VolumeDesc& vol);
    virtual std::string get_dbname();

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

    /**
     * @brief Update volume descriptors
     * This function performs partial update (nblocks, capacity, generation) of the akumuli_volumes
     * table.
     * New volume should be added using the `add_volume` function.
     */
    void upsert_volume_records(std::unordered_map<u32, VolumeDesc>&& input);

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
