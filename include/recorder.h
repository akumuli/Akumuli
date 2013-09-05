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

    struct spa_MemRange {
        void* address;
        int32_t length;
    };


    //! Database instance.
    struct spa_Database { };


    /**
     * Add sample to database.
     * Database must be opend.
     * @param db database instance
     * @param param_id parameter id
     * @param timestamp entry timestamp (unix-time)
     * @param data data
     */
    void spa_add_sample(spa_Database* db, int32_t param_id, int32_t timestamp, spa_MemRange data);


    /**
     * Flush data to disk.
     * @param db database.
     */
    void spa_flush_database(spa_Database* db);


    /** Open existing database.
     */
    spa_Database* spa_open_database(spa_Config config);


    /** Close database.
     */
    void spa_close_database(spa_Database* db);
}
