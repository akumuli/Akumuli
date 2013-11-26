/**
 * PRIVATE HEADER
 *
 * File management API.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */


#pragma once
#include <cstddef>
#include <apr_general.h>
#include <log4cxx/logger.h>

#define MIN_FILE_SIZE (1024*1024)

namespace Akumuli {

struct StorageManager {
    //! Create new database file with predefined size
    static apr_status_t create_database(const char* file_name, size_t size);
    static log4cxx::LoggerPtr s_logger_;
};

}
