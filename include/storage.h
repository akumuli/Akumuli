/**
 * PRIVATE HEADER
 *
 * Page management API.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#pragma once
#include <cstddef>
#include <vector>
#include <queue>
#include <list>
#include <map>
#include <atomic>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "page.h"
#include "util.h"
#include "sequencer.h"
#include "cursor.h"
#include "akumuli_def.h"


namespace Akumuli {

/** Storage volume.
  * Coresponds to one of the storage pages. Includes page
  * data and main memory data.
  */
struct Volume {
    MemoryMappedFile mmap_;
    PageHeader* page_;
    aku_Duration window_;
    size_t max_cache_size_;
    std::unique_ptr<Sequencer> cache_;

    //! Create new volume stored in file
    Volume(const char* file_path, aku_Duration window, size_t max_cache_size, int tag, aku_printf_t logger);

    //! Get pointer to page
    PageHeader* get_page() const;

    //! Reallocate disc space and return pointer to newly mapped page
    PageHeader *reallocate_disc_space();

    //! Open page for writing
    void open();

    //! Flush all data and close volume for write until reallocation
    void close();
};

/** Interface to page manager
 */
struct Storage
{
    typedef std::mutex      LockType;

    // Active volume state
    Volume*                 active_volume_;
    PageHeader*             active_page_;
    std::atomic<int>        active_volume_index_;
    aku_Duration            ttl_;                       //< Late write limit
    std::vector<Volume*>    volumes_;                   //< List of all volumes

    LockType                mutex_;                     //< Storage lock (used by worker thread)

    apr_time_t              creation_time_;  //< Cached metadata
    int                     tag_;  //< Tag to distinct different storage instances
    aku_printf_t            logger_;
    Rand                    rand_;

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_Config const& conf);

    //! Select page that was active last time
    void select_active_page();

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_error(const char* message);

    // Writing

    //! commit changes
    void commit();

    /** Switch volume in round robin manner
      * @param ix current volume index
      */
    void advance_volume_(int ix);

    //! Write data.
    aku_Status write(aku_ParamId param, aku_TimeStamp ts, aku_MemRange data);

    // Reading

    //! Search storage using cursor
    void search(Caller &caller, InternalCursor *cur, SearchQuery const& query) const;

    // Static interface

    /** Create new storage and initialize it.
      * @param storage_name storage name
      * @param metadata_path path to metadata dir
      * @param volumes_path path to volumes dir
      */
    static apr_status_t new_storage(const char* file_name, const char* metadata_path, const char* volumes_path, int num_pages, aku_printf_t logger);

    // Stats
    void get_stats(aku_StorageStats* rcv_stats);
};

}
