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
    TimeDuration window_;
    size_t max_cache_size_;
    std::unique_ptr<Sequencer> cache_;

    //! Create new volume stored in file
    Volume(const char* file_path, TimeDuration window, size_t max_cache_size);

    //! Get pointer to page
    PageHeader* get_page() const noexcept;

    //! Reallocate disc space and return pointer to newly mapped page
    PageHeader *reallocate_disc_space();

    //! Open page for writing
    void open() noexcept;

    //! Flush all data and close volume for write until reallocation
    void close() noexcept;
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
    TimeDuration            ttl_;                       //< Late write limit
    std::vector<Volume*>    volumes_;                   //< List of all volumes

    LockType                mutex_;                     //< Storage lock (used by worker thread)

    // Cached metadata
    apr_time_t creation_time_;

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_Config const& conf);

    //! Select page that was active last time
    void select_active_page();

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_error(const char* message) noexcept;

    // Writing

    //! commit changes
    void commit();

    /** Switch volume in round robin manner
      * @param ix current volume index
      */
    void advance_volume_(int ix) noexcept;

    template<class TEntry>
    int _write_impl(TEntry const& entry) noexcept {
        int status = AKU_WRITE_STATUS_BAD_DATA;
        while(true) {
            int local_rev = active_volume_index_.load();
            TimeSeriesValue ts_value(entry.time, entry.param_id, active_page_->last_offset);
            status = active_page_->add_entry(entry);
            switch (status) {
            case AKU_SUCCESS: {
                Sequencer::Lock merge_lock;
                std::tie(status, merge_lock) = active_volume_->cache_->add(ts_value);
                if (merge_lock.owns_lock()) {
                    // Slow path
                    Caller caller;
                    DirectPageSyncCursor cursor;
                    active_volume_->cache_->merge(caller, &cursor, std::move(merge_lock));
                }
                return status;
            }
            case AKU_EOVERFLOW:
                advance_volume_(local_rev);
                break;  // retry
            case AKU_ELATE_WRITE:
            // Branch for rare and unexpected errors
            default:
                log_error(aku_error_message(status));
                return status;
            };
        }
    }

    //! Write data.
    int write(Entry const& entry);

    //! Write data.
    int write(Entry2 const& entry);

    // Reading

    //! Search storage using cursor
    void search(Caller &caller, InternalCursor *cur, SearchQuery const& query) const noexcept;

    // Static interface

    /** Create new storage and initialize it.
      * @param storage_name storage name
      * @param metadata_path path to metadata dir
      * @param volumes_path path to volumes dir
      */
    static apr_status_t new_storage(const char* file_name, const char* metadata_path, const char* volumes_path, int num_pages);
};

}
