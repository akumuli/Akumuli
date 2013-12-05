/**
 * PRIVATE HEADER
 *
 * Page management API.
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
#include <log4cxx/logger.h>
#include "page.h"
#include "util.h"

namespace Akumuli {

/** Interface to page manager
 */
struct Storage
{
    MemoryMappedFile mmap_;
    PageHeader* metadata_;
    PageHeader* index_;

    Storage(const char* file_name);

    // TODO: add metadata mngmt

    //! get page by index
    PageHeader* get_index_page(int page_index);

    //! commit changes
    void commit();

    //! write data
    void write(Entry const& entry);

    //! write data
    void write(Entry2 const& entry);

    static log4cxx::LoggerPtr s_logger_;
};

}
