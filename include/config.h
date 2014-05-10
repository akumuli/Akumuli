/**
 * PUBLIC HEADER
 *
 * Library configuration data.
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

extern "C" {

/** Library configuration.
 */
struct aku_Config
{
    //! Debug mode trigger
    int32_t debug_mode;

    //! Maximum depth of the late write
    int64_t max_late_write;  // TODO: move to config file

    //! Maximum cache size in bytes
    int64_t max_cache_size;  // TODO: move to config file
};

}
