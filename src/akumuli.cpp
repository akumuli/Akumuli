/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
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

    void flush() {
        storage_.commit();
    }

    void add_sample(uint32_t param_id, int64_t long_timestamp, aku_MemRange value) {
        TimeStamp ts;
        ts.value = long_timestamp;
        auto entry = Entry2(param_id, ts, value);
        storage_.write(entry);
    }

    int32_t find_sample(uint32_t param_id, int64_t instant, aku_MemRange out_data) {
        // FIXME
        throw std::runtime_error("not implemented");
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

void aku_flush_database(aku_Database* db) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->flush();
}

void aku_add_sample(aku_Database* db, uint32_t param_id, int64_t long_timestamp, aku_MemRange value) {
    auto dbi = reinterpret_cast<DatabaseImpl*>(db);
    dbi->add_sample(param_id, long_timestamp, value);
}

int32_t aku_find_sample(aku_Database* db, uint32_t param_id, int64_t instant, aku_MemRange out_data) {
    throw std::runtime_error("not implemented");
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
