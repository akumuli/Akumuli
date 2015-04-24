/** 
 * PUBLIC HEADER
 *
 * Akumuli API.
 * Contains only POD definitions that can be used from "C" code.
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
 */

#pragma once
#include <stdint.h>
#include <apr_errno.h>
#include "akumuli_config.h"
#include "akumuli_def.h"

#ifdef __unix__
#ifdef __cplusplus
#define AKU_EXPORT  extern "C" __attribute__((visibility("default")))
#else
#define AKU_EXPORT
#endif
#else
#define AKU_EXPORT __declspec(dllexport)
#endif

//-----------------
// Data structures
//-----------------

//! Payload data
typedef union {
    const void *ptr;
    double      float64;
    uint64_t    uint64;
} aku_PData;


typedef uint64_t    aku_Timestamp;    //< Timestamp
typedef uint64_t    aku_ParamId;      //< Parameter (or sequence) id
typedef uint64_t    aku_GroupId;      //< Group id
typedef int         aku_Status;       //< Status code of any operation


//! Structure represents memory region
typedef struct {
    void* address;
    uint32_t length;
} aku_MemRange;


//! Database instance.
typedef struct {
    int padding;
} aku_Database;

/**
 * @brief Select search query.
 */
typedef struct {
    //! Begining of the search range
    aku_Timestamp begin;
    //! End of the search range
    aku_Timestamp end;
    //! Number of parameters to search
    uint32_t n_params;
    //! Array of parameters to search
    aku_ParamId params[];
} aku_SelectQuery;


/**
 * @brief The aku_Cursor struct
 */
typedef struct {
    int padding;
} aku_Cursor;


//! Search stats
typedef struct {
    struct {
        uint64_t n_times;               //< How many times interpolation search was performed
        uint64_t n_steps;               //< How many interpolation search steps was performed
        uint64_t n_overshoots;          //< Number of overruns
        uint64_t n_undershoots;         //< Number of underruns
        uint64_t n_matches;             //< Number of matches by interpolation search only
        uint64_t n_reduced_to_one_page;
        uint64_t n_page_in_core_checks; //< Number of page in core checks
        uint64_t n_page_in_core_errors; //< Number of page in core check errors
        uint64_t n_pages_in_core_found; //< Number of page in core found
        uint64_t n_pages_in_core_miss;  //< Number of page misses
    } istats;
    struct {
        uint64_t n_times;               //< How many times binary search was performed
        uint64_t n_steps;               //< How many binary search steps was performed
    } bstats;
    struct {
        uint64_t fwd_bytes;             //< Number of scanned bytes in forward direction
        uint64_t bwd_bytes;             //< Number of scanned bytes in backward direction
    } scan;
} aku_SearchStats;


//! Storage stats
typedef struct {
    uint64_t n_entries;       //< Total number of entries
    uint64_t n_volumes;       //< Total number of volumes
    uint64_t free_space;      //< Free space total
    uint64_t used_space;      //< Space in use
} aku_StorageStats;


//-------------------
// Utility functions
//-------------------

/** This function must be called before any other library function.
  * @param optional_panic_handler function to alternative panic handler
  */
AKU_EXPORT void aku_initialize(aku_panic_handler_t optional_panic_handler);

/** Convert error code to error message.
  * Function returns pointer to statically allocated string
  * there is no need to free it.
  */
AKU_EXPORT const char* aku_error_message(int error_code);

/** Default logger that is used if no logging function is
  * specified. Exported for testing reasons, no need to use it
  * explicitly.
  */
AKU_EXPORT void aku_console_logger(int tag, const char* message);

/**
 * @brief Destroy any object created with aku_make_*** function
 */
AKU_EXPORT void aku_destroy(void* any);


//------------------------------
// Storage management functions
//------------------------------


/**
 * @brief Creates storage for new database on the hard drive
 * @param file_name database file name
 * @param metadata_path path to metadata file
 * @param volumes_path path to volumes
 * @param num_volumes number of volumes to create
 * @return APR errorcode or APR_SUCCESS
 * TODO: move from apr_status_t to aku_Status
 */
AKU_EXPORT apr_status_t aku_create_database
                                ( const char     *file_name
                                , const char     *metadata_path
                                , const char     *volumes_path
                                , int32_t         num_volumes
                                // optional args
                                , uint32_t  compression_threshold
                                , uint64_t  window_size
                                , uint32_t  max_cache_size
                                , aku_logger_cb_t logger
                                );


/** Remove all volumes.
  * @param file_name
  * @param logger
  * @returns status
  */
AKU_EXPORT apr_status_t aku_remove_database(const char* file_name, aku_logger_cb_t logger);


/** Open recenlty create storage.
  * @param path path to storage metadata file
  * @param parameters open parameters
  * @return pointer to new db instance, null if db doesn't exists.
  */
AKU_EXPORT aku_Database* aku_open_database(const char *path, aku_FineTuneParams parameters);


/** Check status of previous open operation
  * @param db pointer to database
  */
AKU_EXPORT aku_Status aku_open_status(aku_Database* db);


//! Close database. Free resources.
AKU_EXPORT void aku_close_database(aku_Database* db);


//---------
// Writing
//---------

/** Write binary blob
  * @param db opened database instance
  * @param param_id paramter id
  * @param timestamp timestamp
  * @param value BLOB memory range
  * @returns operation status
  */
AKU_EXPORT aku_Status aku_write_blob(aku_Database* db, aku_ParamId param_id, aku_Timestamp timestamp, aku_MemRange value);

/** Write measurement to DB
  * @param db opened database instance
  * @param param_id storage parameter id
  * @param timestamp timestamp
  * @param value parameter value
  * @returns operation status
  */
AKU_EXPORT aku_Status aku_write_double_raw(aku_Database* db, aku_ParamId param_id, aku_Timestamp timestamp, double value);

/** Write measurement to DB
  * @param db opened database instance
  * @param series_key_begin should point to the begining of the string with series name
  * @param series_key_end should point to the end of the string
  * @param timestamp timestamp
  * @param value parameter value
  * @returns operation status
  */
AKU_EXPORT aku_Status aku_write_double(aku_Database* db,
                                       const char* series_key_begin,  // TODO: use simple 0-terminated string
                                       const char* series_key_end,
                                       aku_Timestamp timestamp,  // TODO: replace parameter with string
                                       double value);  // TODO: replace parameter with string

//---------
// Queries
//---------

/**
 * @obsolete should be replaced with json query format
 * @brief Create select query with single parameter-id
 */
AKU_EXPORT aku_SelectQuery* aku_make_select_query(aku_Timestamp begin, aku_Timestamp end, uint32_t n_params, aku_ParamId* params);

/**
 * @brief Execute query
 * @param query data structure representing search query
 * @return cursor
 */
AKU_EXPORT aku_Cursor* aku_select(aku_Database* db, aku_SelectQuery* query);

// TODO: AKU_EXPORT aku_Cursor* aku_selecct(aku_Database* db, const char* query);

/**
 * @brief Close cursor
 * @param pcursor pointer to cursor
 */
AKU_EXPORT void aku_close_cursor(aku_Cursor* pcursor);

/**
 * @brief Read data from storage in column-wise manner.
 * @param pcursor pointer to cursor
 * @param timestamps output buffer for storing timestamps
 * @param params output buffer for storing paramids
 * @param pointers output buffer for storing pointers to data
 * @param lengths output buffer for storing lengths of the data items
 * @param array_size specifies size of the all output buffers (it must be the same for all buffers)
 * @note every output parmeter can be null if we doesn't interested in it's value
 */
AKU_EXPORT int aku_cursor_read_columns( aku_Cursor      *pcursor
                                      , aku_Timestamp   *timestamps
                                      , aku_ParamId     *params
                                      , aku_PData       *pointers
                                      , uint32_t        *lengths
                                      , size_t           arrays_size );

//! Check cursor state. Returns zero value if not done yet, non zero value otherwise.
AKU_EXPORT int aku_cursor_is_done(aku_Cursor* pcursor);

//! Check cursor error state. Returns zero value if everything is OK, non zero value otherwise.
AKU_EXPORT int aku_cursor_is_error(aku_Cursor* pcursor, int* out_error_code_or_null);


//--------------------
// Stats and counters
//--------------------

/** Get search counters.
  * @param rcv_stats pointer to `aku_SearchStats` structure that will be filled with data.
  * @param reset reset all counter if not zero
  */
AKU_EXPORT void aku_global_search_stats(aku_SearchStats* rcv_stats, int reset);

/** Get storage stats.
  * @param db database instance.
  * @param rcv_stats pointer to destination
  */
AKU_EXPORT void aku_global_storage_stats(aku_Database *db, aku_StorageStats* rcv_stats);
