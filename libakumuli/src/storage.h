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
#include <apr_dbd.h>
#include <apr.h>
#include <apr_mmap.h>

#include "page.h"
#include "util.h"
#include "sequencer.h"
#include "cursor.h"
#include "akumuli_def.h"

namespace Akumuli {

//! Delete apr pool
void delete_apr_pool(apr_pool_t *p);

//! APR DBD handle deleter
struct AprHandleDeleter {
    const apr_dbd_driver_t *driver;
    AprHandleDeleter(const apr_dbd_driver_t *driver);
    void operator()(apr_dbd_t* handle);
};


//! Time-series index types supported by akumuli
enum SeriesIndex {
    AKU_INDEX_BASIC,  //< basic time-series index for faster graphing and summarization
};


/** Time-series catoegory description in metadata storage.
  * Many different time-series can be stored within the
  * same time-series category.
  */
struct SeriesCategory {
    uint64_t        id;             //< Index type
    std::string     name;           //< Series category name
    std::string     table_name;     //< Series metadata table name
    SeriesIndex     index_type;     //< Index type used for series category
};


/** Time-series schema.
  * Stores all the variety of different time-series categories.
  */
struct Schema {
    std::vector<std::shared_ptr<SeriesCategory>> categories;

    //! C-tor
    template<class FwdIt>
    Schema(FwdIt begin, FwdIt end)
        : categories(begin, end)
    {
    }
};


/** Concrete series description.
  */
struct SeriesInstance {
    uint64_t        id;                         //< Time-series ID
    std::string     name;                       //< Time-series name
    std::weak_ptr<SeriesCategory>   category;   //< Category that hosts this time-series instance
};


/** Sqlite3 backed storage for metadata.
  * Metadata includes:
  * - Volumes list
  * - Conviguration data
  * - Key to id mapping
  */
struct MetadataStorage {
    // Typedefs
    typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)>         PoolT;
    typedef const apr_dbd_driver_t*                                         DriverT;
    typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter>                    HandleT;
    typedef std::pair<int, std::string>                                     VolumeDesc;

    // Members
    PoolT pool_;
    DriverT driver_;
    HandleT handle_;

    /** Create new or open existing db.
      * @throw std::runtime_error in a case of error
      */
    MetadataStorage(const char* db);

    // Creation //

    /** Create tables if database is empty
      * @throw std::runtime_error in a case of error
      */
    void create_tables();

    /** Create new database from schema.
      */
    void create_schema(std::shared_ptr<Schema> schema);

    /** Initialize volumes table
      * @throw std::runtime_error in a case of error
      */
    void init_volumes(std::vector<VolumeDesc> volumes);

    void init_config(uint32_t compression_threshold,
                     uint32_t max_cache_size,
                     uint64_t window_size, const char *creation_datetime);

    // Retreival //

    /** Read list of volumes and their sequence numbers.
      * @throw std::runtime_error in a case of error
      */
    std::vector<VolumeDesc> get_volumes() const;

    void get_configs(uint32_t *compression_threshold,
                     uint32_t *max_cache_size,
                     uint64_t *window_size, std::string *creation_datetime);

private:
    /** Execute query that doesn't return anything.
      * @throw std::runtime_error in a case of error
      * @return number of rows changed
      */
    int execute_query(const char* query);

    typedef std::vector<std::string> UntypedTuple;

    /** Execute select query and return untyped results.
      * @throw std::runtime_error in a case of error
      * @return bunch of strings with results
      */
    std::vector<UntypedTuple> select_query(const char* query) const;
};

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
    aku_logger_cb_t logger_;
    std::atomic_bool is_temporary_;  //< True if this is temporary volume and underlying file should be deleted
    const bool huge_tlb_;

    //! Create new volume stored in file
    Volume(const char           *file_path,
           const aku_Config&     conf,
           bool                  enable_huge_tlb,
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
    aku_Config                config_;
    PVolume                   active_volume_;
    PageHeader*               active_page_;
    std::atomic<int>          active_volume_index_;
    aku_Duration              ttl_;                       //< Late write limit
    bool                      compression;                //< Compression enabled
    aku_Status                open_error_code_;           //< Open op-n error code
    std::vector<PVolume>      volumes_;                   //< List of all volumes

    LockType                  mutex_;                     //< Storage lock (used by worker thread)

    apr_time_t                creation_time_;             //< Cached metadata
    aku_logger_cb_t           logger_;
    Rand                      rand_;
    const uint32_t            durability_;                //< Copy of the durability parameter
    const bool                huge_tlb_;                  //< Copy of enable_huge_tlb parameter

    /** Storage c-tor.
      * @param file_name path to metadata file
      */
    Storage(const char *path, aku_FineTuneParams const& conf);

    //! Select page that was active last time
    void select_active_page();

    //! Prepopulate cache
    void prepopulate_cache(int64_t max_cache_size);

    void log_message(const char* message);

    void log_error(const char* message);

    void log_message(const char* message, uint64_t value);

    // Writing

    /** Switch volume in round robin manner
      * @param ix current volume index
      */
    void advance_volume_(int ix);

    //! Write binary data.
    aku_Status write_blob(aku_ParamId param, aku_TimeStamp ts, aku_MemRange data);

    //! Write double.
    aku_Status write_double(aku_ParamId param, aku_TimeStamp ts, double value);

    aku_Status _write_impl(TimeSeriesValue &value, aku_MemRange data);

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
                                    aku_logger_cb_t    logger);

    /** Remove all volumes
      * @param file_name
      * @param logger
      * @returns status
      */
    static apr_status_t remove_storage(const char* file_name, aku_logger_cb_t logger);

    // Stats
    void get_stats(aku_StorageStats* rcv_stats);

    aku_Status get_open_error() const;
};

}
