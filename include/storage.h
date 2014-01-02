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
#include <memory>

#include "page.h"
#include "util.h"
#include "akumuli_def.h"

namespace Akumuli {

struct ParamCache {
    std::list<EntryOffset> list;
    int64_t last_item_timestamp;
};

// TODO:
class TimeIndex {
    std::map<ParamId, std::unique_ptr<ParamCache>> cache_;
public:

};


/** Storage volume.
  * Coresponds to one of the storage pages. Includes page
  * data and main memory data.
  */
struct Volume {
    MemoryMappedFile mmap_;
    PageHeader* page_;
    TimeIndex main_memory_index_;

    Volume(const char* file_name);

    PageHeader* get_page() const noexcept;

    int reallocate_disc_space() noexcept;
};

/** Interface to page manager
 */
struct Storage
{
    Volume* active_volume_;
    PageHeader* active_page_;
    int active_volume_index_;
    std::vector<Volume*> volumes_;

    // Cached metadata
    apr_time_t creation_time_;
    //

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char* file_name);

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

    // Static interface

    /** Create new storage and initialize it.
      * @param storage_name storage name
      * @param metadata_path path to metadata dir
      * @param volumes_path path to volumes dir
      */
    static apr_status_t new_storage(const char* file_name, const char* metadata_path, const char* volumes_path, int num_pages);
};

}
