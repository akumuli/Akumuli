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

// APR headers
#include <apr.h>
#include <apr_mmap.h>

#include "page.h"
#include "util.h"
#include "sequencer.h"
#include "cursor.h"
#include "seriesparser.h"
#include "akumuli_def.h"
#include "metadatastorage.h"

#include <boost/thread.hpp>

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
    aku_FineTuneParams config_;
    aku_logger_cb_t logger_;
    std::atomic_bool is_temporary_;  //< True if this is temporary volume and underlying file should be deleted

    //! Create new volume stored in file
    Volume(const char           *file_path,
           aku_FineTuneParams    conf,
           aku_logger_cb_t       logger);

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

    //! Make volume read-only
    void make_readonly();

    //! Make volume writeable
    void make_writable();
};

/** Interface to page manager
 */
struct Storage
{
    typedef std::mutex                          LockType;
    typedef std::shared_ptr<Volume>             PVolume;
    typedef std::shared_ptr<MetadataStorage>    PMetadataStorage;
    typedef std::shared_ptr<SeriesMatcher>      PSeriesMatcher;
    typedef std::shared_ptr<ChunkCache>         PCache;

    // Active volume state
    aku_FineTuneParams        config_;
    PVolume                   active_volume_;
    PageHeader*               active_page_;
    std::atomic<int>          active_volume_index_;
    aku_Duration              ttl_;                       //< Late write limit
    aku_Status                open_error_code_;           //< Open op-n error code
    std::vector<PVolume>      volumes_;                   //< List of all volumes
    PMetadataStorage          metadata_;                  //< Metadata storage
    PSeriesMatcher            matcher_;                   //< Series matcher

    LockType                  mutex_;                     //< Storage lock (used by worker thread)

    apr_time_t                creation_time_;             //< Cached metadata
    aku_logger_cb_t           logger_;
    Rand                      rand_;
    PCache                    cache_;

    //! Local (per query) string pool
    boost::thread_specific_ptr<SeriesMatcher> local_matcher_;

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_FineTuneParams const& conf);

    void set_thread_local_matcher(SeriesMatcher* spool);

    //! Select page that was active last time
    void select_active_page();

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_message(const char* message) const;

    void log_error(const char* message) const;

    void log_message(const char* message, uint64_t value) const;

    // Writing

    //! Close db (this call should be performed by writer thread)
    void close();

    /** Switch volume in round robin manner
      * @param ix current volume index
      */
    void advance_volume_(int ix);

    //! Write double.
    aku_Status write_double(aku_ParamId param, aku_Timestamp ts, double value);

    aku_Status _write_impl(TimeSeriesValue value, aku_MemRange data);

    /** Convert series name to parameter id
      * @param begin should point to series name
      * @param end should point to series name end
      * @param value is a pointer to output parameter
      * @returns status code
      */
    aku_Status series_to_param_id(const char* begin, const char* end, uint64_t *value);

    /** Convert parameter id to series name.
      */
    int param_id_to_series(aku_ParamId id, char* buffer, size_t buffer_size) const;

    // Reading

    //! Search storage using cursor
    void search(Caller &caller, InternalCursor* cur, const char* query) const;

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
                                    aku_logger_cb_t logger);

    /** Remove all volumes
      * @param file_name
      * @param logger
      * @returns status
      */
    static apr_status_t remove_storage(const char* file_name, aku_logger_cb_t logger);

    // Stats
    void get_stats(aku_StorageStats* rcv_stats);

    aku_Status get_open_error() const;

    void debug_print() const;
};

}
