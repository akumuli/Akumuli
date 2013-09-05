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
struct spa_Config
{
    char* path_to_file;
    int32_t page_size;
    int32_t debug_mode;
};

}
