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
#include <vector>
#include <queue>
#include <list>
#include <map>
#include <atomic>

#include <log4cxx/logger.h>
#include "page.h"
#include "util.h"
#include "akumuli_def.h"

namespace Akumuli {

namespace details {

struct ParamCache {
    std::list<EntryOffset> list;
    int64_t last_item_timestamp;
};

}

/** Interface to page manager
 */
struct Storage
{
    MemoryMappedFile mmap_;
    PageHeader* metadata_;
    PageHeader* active_page_;  // previously known as index_
    int active_page_index_;
    std::vector<PageHeader*> page_cache_;

    std::map<ParamId, details::ParamCache> in_memory_cache_;

    // Cached metadata
    apr_time_t creation_time_;
    //

    Storage(const char* file_name);

    //! get page by index
    PageHeader* get_index_page(int page_index);

    // Writing

    //! commit changes
    void commit();

    /** Write data.
      */
    int write(Entry const& entry);

    //! write data
    int write(Entry2 const& entry);

    // Reading

    void find_entry(ParamId param, TimeStamp time);

    // File management

    //! Create empty file
    static apr_status_t create_storage(const char* file_name, int num_pages);

    //! Create akumuli database from file (file must be created using `create_storage` method)
    static apr_status_t init_storage(const char* file_name);

private:
    static log4cxx::LoggerPtr s_logger_;
};

}
