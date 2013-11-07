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
struct DatabaseImpl : public aku_Database
{
    std::string _path_to_file;
    size_t _page_size;
    bool _debug_mode;

    // private fields
    DatabaseImpl(const aku_Config& config)
        : _path_to_file(config.path_to_file)
        , _page_size(config.page_size)
        , _debug_mode(config.debug_mode != 0)
    {
    }
};


void aku_flush_database(aku_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    printf("%s", dbi->_path_to_file.c_str());
}


void aku_add_sample(aku_Database* db, int32_t param_id, int32_t unix_timestamp, aku_MemRange value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
}


aku_Database* aku_open_database(aku_Config config)
{
    aku_Database* ptr = new DatabaseImpl(config);
    return static_cast<aku_Database*>(ptr);
}

void aku_close_database(aku_Database* db)
{
    delete db;
}
