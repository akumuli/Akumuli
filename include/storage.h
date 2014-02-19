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
#include "cache.h"
#include "cursor.h"
#include "akumuli_def.h"

namespace Akumuli {


/** Database cursor base class.
  * @code
  * auto pcursor = create_specific_cursor(...);
  * while(!pcursor->done())
  *     storage->search(pcursor);
  * if(pcursor->status() != AKU_SUCCESS)
  *     report_error(pcursor->status());
  * @endcode
  */
struct Cursor {
    ~Cursor() {}
    virtual size_t read(EntryOffset* out_buf, size_t out_buf_len) = 0;
    virtual bool done() const = 0;
    virtual int status() const = 0;
};


/** Storage volume.
  * Coresponds to one of the storage pages. Includes page
  * data and main memory data.
  */
struct Volume {
    MemoryMappedFile mmap_;
    PageHeader* page_;
    TimeDuration ttl_;
    size_t max_cache_size_;
    std::unique_ptr<Cache> cache_;

    //! Create new volume stored in file
    Volume(const char* file_path, TimeDuration ttl, size_t max_cache_size);

    //! Get pointer to page
    PageHeader* get_page() const noexcept;

    //! Reallocate disc space and return pointer to newly mapped page
    PageHeader *reallocate_disc_space();

    //! Flush all data and close volume for write until reallocation
    void close() noexcept;
};

/** Interface to page manager
 */
struct Storage
{
    Volume* active_volume_;
    PageHeader* active_page_;
    int active_volume_index_;
    TimeDuration ttl_;
    std::vector<Volume*> volumes_;

    // Cached metadata
    apr_time_t creation_time_;
    //

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(aku_Config const& conf);

    // Writing

    //! commit changes
    void commit();

    /** Write data.
      */
    int write(Entry const& entry);

    //! write data
    int write(Entry2 const& entry);

    // Reading

    //! Search storage using cursor
    void search(BasicCursor* cursor);

    // Static interface

    /** Create new storage and initialize it.
      * @param storage_name storage name
      * @param metadata_path path to metadata dir
      * @param volumes_path path to volumes dir
      */
    static apr_status_t new_storage(const char* file_name, const char* metadata_path, const char* volumes_path, int num_pages);
};

}
