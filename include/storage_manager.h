/**
 * PRIVATE HEADER
 *
 * Storage management API.
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
#include <memory>
#include <apr_general.h>
#include <log4cxx/logger.h>

#include "storage.h"


namespace Akumuli {

// TODO: merge it with 'Storage' class
struct StorageManager {
    //! Create new database file with predefined size
    static apr_status_t create_storage(const char* file_name, int num_pages);
    static apr_status_t init_storage(const char* file_name);
    static log4cxx::LoggerPtr s_logger_;
};

}
