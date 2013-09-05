/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */


#include "recorder.h"
#include <cstdio>
#include <cstdlib>
#include <string>


/** 
 * Object that extends a Database struct.
 * Can be used from "C" code.
 */
struct DatabaseImpl : public spa_Database
{
    std::string _path_to_file;
    size_t _page_size;
    bool _debug_mode;

    // private fields
    DatabaseImpl(const spa_Config& config)
        : _path_to_file(config.path_to_file)
        , _page_size(config.page_size)
        , _debug_mode(config.debug_mode != 0)
    {
    }
};


void spa_flush_database(spa_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    printf("%s", dbi->_path_to_file.c_str());
}


void spa_add_sample(spa_Database* db, int32_t param_id, int32_t unix_timestamp, spa_MemRange value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
}


spa_Database* spa_open_database(spa_Config config)
{
    spa_Database* ptr = new DatabaseImpl(config);
    return static_cast<spa_Database*>(ptr);
}

void spa_close_database(spa_Database* db)
{
    delete db;
}
