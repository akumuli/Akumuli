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
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "page.h"
#include "util.h"
#include "cache.h"
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
    TimeDuration ttl_;
    size_t max_cache_size_;
    std::unique_ptr<Cache> cache_;

    //! Create new volume stored in file
    Volume(const char* file_path, TimeDuration ttl, size_t max_cache_size);

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
    typedef tbb::spin_mutex PageLock;

    // Active volume state
    Volume*                 active_volume_;
    PageHeader*             active_page_;
    std::atomic<int>        active_volume_index_;
    TimeDuration            ttl_;                       //< Late write limit
    std::vector<Volume*>    volumes_;                   //< List of all volumes

    LockType                mutex_;                     //< Storage lock (used by worker thread)
    PageLock                page_mutex_;

    // Worker thread state
    std::queue<Volume*>     outgoing_;                  //< Write back queue
    std::condition_variable worker_condition_;
    std::thread             worker_;
    bool                    stop_worker_;               //< Completion flag, set to stop worker

    // Cached metadata
    apr_time_t creation_time_;
    //

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_Config const& conf);

    //! Select page that was active last time
    void select_active_page();

    template<class Lock>
    void wait_queue_state_(Lock& l, bool is_empty) {
        worker_condition_.wait(l, [this, is_empty]() {
            return this->outgoing_.empty() == is_empty;
        });
    }

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_error(const char* message) noexcept;

    void run_worker_() noexcept;

    // Writing

    //! commit changes
    void commit();

    void notify_worker_(std::unique_lock<LockType>& lock, size_t ntimes, Volume* volume) noexcept;

    /** Switch volume in round robin manner
      * @param ix current volume index
      */
    void advance_volume_(int ix) noexcept;

    template<class TEntry>
    int _write_impl(TEntry const& entry) noexcept {
        // TODO: review!
        int status = AKU_WRITE_STATUS_BAD_DATA;
        while(true) {
            int local_rev = active_volume_index_.load();
            size_t nswaps = 0;
            status = active_volume_->cache_->add_entry(entry, active_page_->last_offset, &nswaps);
            switch (status) {
            case AKU_SUCCESS: {
                // Page write is single threaded. Mutex needed
                // to sync worker thread with writer thread and
                // all reader threads.
                page_mutex_.lock();
                status = active_page_->add_entry(entry);
                page_mutex_.unlock();
                switch (status) {
                case AKU_WRITE_STATUS_SUCCESS:
                    if (nswaps) {
                        std::unique_lock<LockType> guard(mutex_);
                        notify_worker_(guard, nswaps, active_volume_);
                    }
                    // Fast path
                    return status;
                case AKU_WRITE_STATUS_OVERFLOW:
                    // Slow path
                    advance_volume_(local_rev);
                    continue;
                };
            }
            // Errors that can be very frequent
            case AKU_EOVERFLOW:
            case AKU_ELATE_WRITE:
                break;
            // Branch for rare and unexpected errors
            default:
                log_error(aku_error_message(status));
                break;
            };
        }
        return status;
    }

    //! Write data.
    int write(Entry const& entry);

    //! Write data.
    int write(Entry2 const& entry);

    // Reading

    //! Search storage using cursor
    void search(InternalCursor* cursor);

    // Static interface

    /** Create new storage and initialize it.
      * @param storage_name storage name
      * @param metadata_path path to metadata dir
      * @param volumes_path path to volumes dir
      */
    static apr_status_t new_storage(const char* file_name, const char* metadata_path, const char* volumes_path, int num_pages);
};

}
