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
struct Volume : std::enable_shared_from_this<Volume>
{
    MemoryMappedFile mmap_;
    PageHeader* page_;
    aku_Duration window_;
    size_t max_cache_size_;
    std::unique_ptr<Sequencer> cache_;
    std::string file_path_;
    const aku_Config& config_;
    const int tag_;
    aku_printf_t logger_;
    std::atomic_bool is_temporary_;  //< True if this is temporary volume and underlying file should be deleted

    //! Create new volume stored in file
    Volume(const char* file_path, const aku_Config &conf, int tag, aku_printf_t logger);

    ~Volume();

    //! Get pointer to page
    PageHeader* get_page() const;

    //! Reallocate space safely
    std::shared_ptr<Volume> safe_realloc();

    //! Open page for writing
    void open();

    //! Flush all data and close volume for write until reallocation
    void close();

    //! Flush page
    void flush();

    //! Search volume page (not cache)
    void search(Caller& caller, InternalCursor* cursor, SearchQuery query) const;
};

/** Interface to page manager
 */
struct Storage
{
    typedef std::mutex      LockType;
    typedef std::shared_ptr<Volume> PVolume;

    // Active volume state
    aku_Config                 config_;
    aku_FineTuneParams const& params_;
    PVolume                   active_volume_;
    PageHeader*               active_page_;
    std::atomic<int>          active_volume_index_;
    aku_Duration              ttl_;                       //< Late write limit
    bool                      compression;                //< Compression enabled
    std::vector<PVolume>      volumes_;                   //< List of all volumes

    LockType                  mutex_;                     //< Storage lock (used by worker thread)

    apr_time_t                creation_time_;             //< Cached metadata
    int                       tag_;                       //< Tag to distinct different storage instances
    aku_printf_t              logger_;
    Rand                      rand_;

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_FineTuneParams const& conf);

    //! Select page that was active last time
    void select_active_page();

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_error(const char* message);

    void log_message(const char* message);

    void log_message(const char* message, uint64_t value);

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
    static apr_status_t new_storage(const char     *file_name,
                                    const char     *metadata_path,
                                    const char     *volumes_path,
                                    int             num_pages,
                                    uint32_t compression_threshold,
                                    uint64_t window_size,
                                    uint32_t max_cache_size,
                                    aku_printf_t    logger);

    // Stats
    void get_stats(aku_StorageStats* rcv_stats);
};

}
