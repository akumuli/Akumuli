/**
 * PRIVATE HEADER
 *
 * Descriptions of internal data structures used to store data in memory mappaed files.
 * All data are in host byte order.
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
#include <cstdint>
#include <functional>
#include <vector>
#include <mutex>
#include "akumuli.h"
#include "util.h"
#include "internal_cursor.h"
#include "compression.h"

const int64_t AKU_MAX_PAGE_SIZE   = 0x100000000;
const int64_t AKU_MAX_PAGE_OFFSET =  0xFFFFFFFF;

namespace Akumuli {

typedef uint64_t aku_Duration;     //< Time duration
// NOTE: Obsolete
typedef uint32_t aku_EntryOffset;  //< Entry offset

struct ChunkDesc {
    uint32_t n_elements;        //< Number of elements in a chunk
    uint32_t begin_offset;      //< Data begin offset
    uint32_t end_offset;        //< Data end offset
    uint32_t checksum;          //< Checksum
} __attribute__((packed));

//! Storage configuration
struct aku_Config {
    uint32_t compression_threshold;

    //! Maximum depth of the late write
    uint64_t window_size;

    //! Maximum cache size in bytes
    uint32_t max_cache_size;
};

struct aku_Entry {
    aku_Timestamp  time;      //< Entry timestamp
    aku_ParamId    param_id;  //< Parameter ID
    uint32_t       length;    //< Entry length: constant + variable sized parts
    uint32_t       value[];   //< Data begining
} __attribute__((packed));

//! PageHeader forward declaration
struct PageHeader;


/** Page bounding box.
 *  All data is two dimentional: param-timestamp.
 */
struct PageBoundingBox {
    aku_ParamId max_id;
    aku_ParamId min_id;
    aku_Timestamp max_timestamp;
    aku_Timestamp min_timestamp;

    PageBoundingBox();
};


struct PageHistogramEntry {
    aku_Timestamp timestamp;
    uint32_t index;
};


/** Page histogram for approximation search */
struct PageHistogram {
    uint32_t size;
    PageHistogramEntry entries[AKU_HISTOGRAM_SIZE];
};

struct SearchStats {
    aku_SearchStats stats;
    std::mutex mutex;

    SearchStats() {
        memset(&stats, 0, sizeof(stats));
    }
};

SearchStats& get_global_search_stats();


/** Search query
  * @obsolete would be replaced with query processor
  */
struct SearchQuery {

    // ParamId match type
    enum ParamMatch {
        LT_ALL,         //< This value is less then all values of interest
        GT_ALL,         //< This value is greather than all values of interest
        NO_MATCH,       //< This valued doesn't match but it nither greather nither less than all values of interest
        MATCH           //< This value matches
    };

    // Matcher function must compare paramId with values of interest and return
    // LT_ALL if all values of interest is greather than paramId, GT_ALL if all
    // values of interest is less than paramId, MATCH - if paramId matches one
    // or NO_MATCH in all other cases.
    // Matcher f-n can return only MATCH and NO_MATCH. Search algorithms doesn't
    // need to rely on first two values of the enumeration (LT_ALL and GT_ALL).
    // This is just a hint to the search algorithm that can speedup search.
    typedef std::function<ParamMatch(aku_ParamId)> MatcherFn;

    // search query
    aku_Timestamp lowerbound;     //< begining of the time interval (0 for -inf) to search
    aku_Timestamp upperbound;     //< end of the time interval (0 for inf) to search
    MatcherFn     param_pred;     //< parmeter search predicate
    int            direction;     //< scan direction

    /** Query c-tor for single parameter searching
     *  @param pid parameter id
     *  @param low time lowerbound (0 for -inf)
     *  @param upp time upperbound (MAX_TIMESTAMP for inf)
     *  @param scan_dir scan direction
     */
    SearchQuery( aku_ParamId      param_id
               , aku_Timestamp    low
               , aku_Timestamp    upp
               , int              scan_dir);


    /** Query c-tor
     *  @param matcher parameter matcher
     *  @param low time lowerbound (0 for -inf)
     *  @param upp time upperbound (MAX_TIMESTAMP for inf)
     *  @param scan_dir scan direction
     */
    SearchQuery( MatcherFn     matcher
               , aku_Timestamp low
               , aku_Timestamp upp
               , int           scan_dir);
};


/**
 * In-memory page representation.
 * PageHeader represents begining of the page.
 * Entry indexes grows from low to high adresses.
 * Entries placed in the bottom of the page.
 * This class must be nonvirtual.
 */
struct PageHeader {
    typedef std::tuple<aku_Timestamp, SearchQuery const&, uint32_t> CursorContext;
    // metadata
    const uint32_t version;     //< format version
    uint32_t count;             //< number of elements stored
    uint32_t last_offset;       //< offset of the last added record
    uint32_t sync_count;        //< index of the last synchronized record
    uint32_t checkpoint;        //< page checkpoint index
    uint32_t open_count;        //< how many times page was open for write
    uint32_t close_count;       //< how many times page was closed for write
    uint32_t page_id;           //< page index in storage
    uint64_t length;            //< page size
    // NOTE: maybe it is possible to get this data from page_index?
    PageBoundingBox bbox;       //< page data limits
    PageHistogram histogram;    //< histogram
    aku_EntryOffset page_index[];   //< page index

    //! Convert entry index to entry offset
    std::pair<aku_EntryOffset, int> index_to_offset(uint32_t index) const;

    //! Get const pointer to the begining of the page
    const char* cdata() const;

    //! Get pointer to the begining of the page
    char* data();

    void update_bounding_box(aku_ParamId param, aku_Timestamp time);

    //! C-tor
    PageHeader(uint32_t count, uint64_t length, uint32_t page_id);

    //! Clear all page conent (open_count += 1)
    void reuse();

    //! Close page for write (close_count += 1)
    void close();

    //! Return number of entries stored in page
    int get_entries_count() const;

    //! Returns amount of free space in bytes
    size_t get_free_space() const;

    bool inside_bbox(aku_ParamId param, aku_Timestamp time) const;

    /**
     * Add new entry to page data.
     * @param entry entry
     * @returns operation status
     */
    int add_entry(const aku_ParamId param, const aku_Timestamp timestamp, const aku_MemRange data);

    /**
     * Add some data to last entry. (without length)
     * @param data data element
     * @param free_space_required minimum amount of space inside the page
     * @returns operation status
     */
    int add_chunk(const aku_MemRange data, const uint32_t free_space_required);

    /**
     * Complete chunk. Add compressed header and index.
     * @param data chunk header data (list of sorted timestamps, param ids, offsets and lengths
     * @returns operation status
     */
    int complete_chunk(const ChunkHeader& data);

    /**
     * Get length of the entry.
     * @param entry_index index of the entry.
     * @returns 0 if index is out of range, entry length otherwise.
     */
    int get_entry_length_at(int entry_index) const;

    /**
     * Get length of the entry.
     * @param offset offset of the entry.
     * @returns 0 if index is out of range, entry length otherwise.
     */
    int get_entry_length(aku_EntryOffset offset) const;

    /**
     * Copy entry from page to receiving buffer using index.
     * @param receiver receiving buffer
     * receiver.length must contain correct buffer length
     * buffer length can be larger than sizeof(Entry)
     * @returns 0 if index out of range, -1*entry[index].length
     * if buffer is to small, entry[index].length on success.
     */
    int copy_entry_at(int index, aku_Entry* receiver) const;

    /**
     * Copy entry from page to receiving buffer using offset.
     * @param receiver receiving buffer
     * receiver.length must contain correct buffer length
     * buffer length can be larger than sizeof(Entry)
     * @returns 0 if index out of range, -1*entry[index].length
     * if buffer is to small, entry[index].length on success.
     */
    int copy_entry(aku_EntryOffset offset, aku_Entry* receiver) const;

    /**
     * Get pointer to entry without copying using index
     * @param index entry index
     * @returns pointer to entry or NULL
     */
    const aku_Entry* read_entry_at(uint32_t index) const;

    /**
     * Get pointer to entry without copying using offset
     * @param entry offset
     * @returns pointer to entry or NULL
     */
    const aku_Entry* read_entry(aku_EntryOffset offset) const;

    /**
     * Get pointer to entry data without copying using
     * data offset.
     * @param data offset
     * @returns pointer to entry data or NULL
     */
    const void* read_entry_data(aku_EntryOffset offset) const;

    /**
      * Execute search query. Results are sent to cursor.
      */
    void search(Caller& caller, InternalCursor* cursor, SearchQuery query) const;

    // Only for testing
    void _sort();

    /** Update page index.
      * @param offsets ordered offsets
      * @param num_offsets number of values in buffer
      */
    void sync_next_index(aku_EntryOffset offsets, uint32_t rand_val, bool sort_histogram);

    static void get_search_stats(aku_SearchStats* stats, bool reset=false);
};

}  // namespaces
