/** 
 * PUBLIC HEADER
 *
 * Akumuli API.
 * Contains only POD definitions that can be used from "C" code.
 *
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
#include <cstdint>
#include <apr_errno.h>
#include "config.h"
#include "akumuli_def.h"

extern "C" {

    typedef uint32_t EntryOffset;
    typedef uint32_t ParamId;

    struct aku_MemRange {
        void* address;
        uint32_t length;
    };

    const char* aku_error_message(int error_code);

    //! Database instance.
    struct aku_Database { };

    /**
     * @brief Creates storage for new database
     * @param file_name database file name
     * @param metadata_path path to metadata file
     * @param volumes_path path to volumes
     * @param num_volumes number of volumes to create
     * @return APR errorcode or APR_SUCCESS
     */
    apr_status_t create_database( const char* 	file_name
                                , const char* 	metadata_path
                                , const char* 	volumes_path
                                , int32_t       num_volumes
                                );
    /**
     * @brief Add sample to database.
     * Database must be opend.
     * @param db database instance
     * @param param_id parameter id
     * @param timestamp entry timestamp (64-bit timestamp)
     * @param data data
     */
    void aku_add_sample( aku_Database*     db
                       , uint32_t          param_id
                       , int64_t           timestamp
                       , aku_MemRange      data
                       );

    /**
     * @brief Find value of the parameter.
     * Database must be opend.
     * @param db database instance
     * @param param_id parameter id
     * @param instant time of interest (0 for current time)
     * @param out_data output data
     * @brief out_data format - interleaved
     * length, timestamp and parameter values aligned by byte.
     * @returns 0 on success -min_length if out_data is not larage enough
     */
    int32_t aku_find_sample
                        ( aku_Database*     db
                        , uint32_t          param_id
                        , int64_t           instant
                        , aku_MemRange      out_data
                        );
    /**
     * @brief Find all values in time range.
     * Database must be opend.
     * @param db database instance.
     * @param param_id parameter id
     * @param begin begining of the range (0 for -inf)
     * @param end end of the range (0 for inf)
     * @param out_data output data
     * @brief out_data format - interleaved
     * length, timestamp and parameter values aligned by byte.
     * @returns num results on success -min_length if out_data is not larage enough
    */
    /*
    int32_t aku_find_samples
                        ( aku_Database*     db
                        , uint32_t          param_id
                        , int64_t           begin,
                        , int64_t           end,
                        , aku_MemRange      out_data
                        );
    */

    /**
     * @brief Flush data to disk.
     * @param db database.
     */
    void aku_flush_database(aku_Database* db);


    /** Open existing database.
     */
    aku_Database* aku_open_database(const char *path, aku_Config config);


    /** Close database.
     */
    void aku_close_database(aku_Database* db);
}
