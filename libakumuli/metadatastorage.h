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
#include <vector>
#include <memory>

#include <apr.h>
#include <apr_dbd.h>

#include "akumuli.h"
#include "akumuli_def.h"
#include "seriesparser.h"

namespace Akumuli {

//! Delete apr pool
void delete_apr_pool(apr_pool_t *p);

//! APR DBD handle deleter
struct AprHandleDeleter {
    const apr_dbd_driver_t *driver;
    AprHandleDeleter(const apr_dbd_driver_t *driver);
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
    typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)>         PoolT;
    typedef const apr_dbd_driver_t*                                         DriverT;
    typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter>                    HandleT;
    typedef std::pair<int, std::string>                                     VolumeDesc;
    typedef apr_dbd_prepared_t*                                             PreparedT;

    // Members
    PoolT           pool_;
    DriverT         driver_;
    HandleT         handle_;
    PreparedT       insert_;
    aku_logger_cb_t logger_;

    /** Create new or open existing db.
      * @throw std::runtime_error in a case of error
      */
    MetadataStorage(const char* db, aku_logger_cb_t logger);

    // Creation //

    /** Create tables if database is empty
      * @throw std::runtime_error in a case of error
      */
    void create_tables();

    /** Initialize volumes table
      * @throw std::runtime_error in a case of error
      */
    void init_volumes(std::vector<VolumeDesc> volumes);

    void init_config(const char *creation_datetime);

    // Retreival //

    /** Read list of volumes and their sequence numbers.
      * @throw std::runtime_error in a case of error
      */
    std::vector<VolumeDesc> get_volumes() const;

    void get_configs(std::string *creation_datetime);

    /** Read larges series id */
    uint64_t get_prev_largest_id();

    aku_Status load_matcher_data(SeriesMatcher& matcher);

    // Writing //

    typedef std::tuple<const char*, int, uint64_t> SeriesT;

    /** Add new series to the metadata storage.
      */
    void insert_new_names(std::vector<SeriesT> items);

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
