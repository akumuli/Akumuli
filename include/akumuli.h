/** 
 * PUBLIC HEADER
 *
 * Spatium API.
 * Contains only POD definitions that can be used from "C" code.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */

#pragma once
#include <cstdint>
#include "config.h"

extern "C" {

    struct aku_MemRange {
        void* address;
        uint32_t length;
    };


    //! Database instance.
    struct aku_Database { };

    /**
     * Add sample to database.
     * Database must be opend.
     * @param db database instance
     * @param param_id parameter id
     * @param timestamp entry timestamp (64-bit timestamp)
     * @param data data
     */
    void aku_add_sample
                        ( aku_Database*     db
                        , uint32_t          param_id
                        , int64_t           timestamp
                        , aku_MemRange      data
                        );

    /**
     * Find value of the parameter.
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
     * Find all values in time range.
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
     * Flush data to disk.
     * @param db database.
     */
    void aku_flush_database(aku_Database* db);


    /** Open existing database.
     */
    aku_Database* aku_open_database(aku_Config config);


    /** Close database.
     */
    void aku_close_database(aku_Database* db);
}
