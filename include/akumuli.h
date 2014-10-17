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
#include <cstdint>
#include <apr_errno.h>
#include "config.h"
#include "akumuli_def.h"

#ifdef __unix__
#define AKU_EXPORT __attribute__((visibility("default")))
#else
#define AKU_EXPORT __declspec(dllexport)
#endif

extern "C" {

    //-----------------
    // Data structures
    //-----------------

    typedef uint64_t    aku_TimeStamp;    //< Timestamp
    typedef uint64_t    aku_ParamId;      //< Parameter (or sequence) id
    typedef int         aku_Status;       //< Status code of any operation
    typedef const void* aku_PData;


    //! Structure represents memory region
    struct aku_MemRange {
        void* address;
        uint32_t length;
    };


    //! Database instance.
    struct aku_Database {};

    /**
     * @brief Select search query.
     */
    struct aku_SelectQuery {
        //! Begining of the search range
        aku_TimeStamp begin;
        //! End of the search range
        aku_TimeStamp end;
        //! Number of parameters to search
        uint32_t n_params;
        //! Array of parameters to search
        aku_ParamId params[];
    };


    /**
     * @brief The aku_Cursor struct
     */
    struct aku_Cursor {};


    //! Search stats
    struct aku_SearchStats {
        struct InterpolationStats {
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
        struct BinarySearch {
            uint64_t n_times;               //< How many times binary search was performed
            uint64_t n_steps;               //< How many binary search steps was performed
        } bstats;
        struct Scan {
            uint64_t fwd_bytes;             //< Number of scanned bytes in forward direction
            uint64_t bwd_bytes;             //< Number of scanned bytes in backward direction
        } scan;
    };


    //! Storage stats
    struct aku_StorageStats {
        uint64_t n_entries;       //< Total number of entries
        uint64_t n_volumes;       //< Total number of volumes
        uint64_t free_space;      //< Free space total
        uint64_t used_space;      //< Space in use
    };


    //-------------------
    // Utility functions
    //-------------------

    /** This function must be called before any other library function.
      * @param optional_panic_handler function to alternative panic handler
      */
    AKU_EXPORT void aku_initialize(aku_panic_handler_t optional_panic_handler=0);

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
     */
    AKU_EXPORT apr_status_t aku_create_database( const char*  file_name
                                    , const char*  metadata_path
                                    , const char*  volumes_path
                                    , int32_t      num_volumes
                                    // optional args
                                    , const uint32_t *compression_threshold
                                    , const uint64_t *window_size
                                    , const uint32_t *max_cache_size
                                    , aku_printf_t logger
                                    );

    /** Open recenlty create storage.
      * @param path path to storage metadata file
      * @param parameters open parameters
      * @return pointer to new db instance, null if db doesn't exists.
      */
    AKU_EXPORT aku_Database* aku_open_database(const char *path, aku_FineTuneParams parameters);

    //! Close database. Free resources.
    AKU_EXPORT void aku_close_database(aku_Database* db);


    //---------
    // Writing
    //---------

    AKU_EXPORT aku_Status aku_write(aku_Database* db, aku_ParamId param_id, aku_TimeStamp long_timestamp, aku_MemRange value);


    //---------
    // Queries
    //---------

    /**
     * @brief Create select query with single parameter-id
     */
    AKU_EXPORT aku_SelectQuery* aku_make_select_query(aku_TimeStamp begin, aku_TimeStamp end, uint32_t n_params, aku_ParamId* params);

    /**
     * @brief Execute query
     * @param query data structure representing search query
     * @return cursor
     */
    AKU_EXPORT aku_Cursor* aku_select(aku_Database* db, aku_SelectQuery* query);

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
                                          , aku_TimeStamp   *timestamps
                                          , aku_ParamId     *params
                                          , aku_PData       *pointers
                                          , uint32_t        *lengths
                                          , size_t           arrays_size );

    //! Check cursor state.
    AKU_EXPORT bool aku_cursor_is_done(aku_Cursor* pcursor);

    //! Check cursor error state.
    AKU_EXPORT bool aku_cursor_is_error(aku_Cursor* pcursor, int* out_error_code_or_null);


    //--------------------
    // Stats and counters
    //--------------------

    /** Get search counters.
      * @param rcv_stats pointer to `aku_SearchStats` structure that will be filled with data.
      * @param reset reset all counter if true
      */
    AKU_EXPORT void aku_global_search_stats(aku_SearchStats* rcv_stats, bool reset=false);

    /** Get storage stats.
      * @param db database instance.
      * @param rcv_stats pointer to destination
      */
    AKU_EXPORT void aku_global_storage_stats(aku_Database *db, aku_StorageStats* rcv_stats);
}
