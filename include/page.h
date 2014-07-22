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
#include "akumuli.h"
#include "util.h"
#include "internal_cursor.h"

const int64_t AKU_MAX_PAGE_SIZE   = 0x100000000;
const int64_t AKU_MAX_PAGE_OFFSET =  0xFFFFFFFF;

namespace Akumuli {

typedef std::pair<aku_EntryOffset, const PageHeader*> CursorResult;

std::ostream& operator << (std::ostream& st, CursorResult res);

/** Page bounding box.
 *  All data is two dimentional: param-timestamp.
 */
struct PageBoundingBox {
    aku_ParamId max_id;
    aku_ParamId min_id;
    aku_TimeStamp max_timestamp;
    aku_TimeStamp min_timestamp;

    PageBoundingBox();
};


/** Search query */
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
    aku_TimeStamp lowerbound;     //< begining of the time interval (0 for -inf)
    aku_TimeStamp upperbound;     //< end of the time interval (0 for inf)
    MatcherFn     param_pred;     //< parmeter search predicate
    int            direction;      //< scan direction

    /** Query c-tor for single parameter searching
     *  @param pid parameter id
     *  @param low time lowerbound (0 for -inf)
     *  @param upp time upperbound (MAX_TIMESTAMP for inf)
     *  @param scan_dir scan direction
     */
    SearchQuery( aku_ParamId      param_id
               , aku_TimeStamp    low
               , aku_TimeStamp    upp
               , int              scan_dir);


    /** Query c-tor
     *  @param matcher parameter matcher
     *  @param low time lowerbound (0 for -inf)
     *  @param upp time upperbound (MAX_TIMESTAMP for inf)
     *  @param scan_dir scan direction
     */
    SearchQuery( MatcherFn     matcher
               , aku_TimeStamp low
               , aku_TimeStamp upp
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
    // metadata
    uint32_t count;             //< number of elements stored
    uint32_t last_offset;       //< offset of the last added record
    uint32_t sync_count;        //< index of the last synchronized record
    uint64_t length;            //< page size
    uint32_t open_count;        //< how many times page was open for write
    uint32_t close_count;       //< how many times page was closed for write
    uint32_t page_id;           //< page index in storage
    // NOTE: maybe it is possible to get this data from page_index?
    PageBoundingBox bbox;       //< page data limits
    aku_EntryOffset page_index[];   //< page index

    //! Convert entry index to entry offset
    std::pair<aku_EntryOffset, int> index_to_offset(uint32_t index) const;

    //! Get const pointer to the begining of the page
    const char* cdata() const;

    //! Get pointer to the begining of the page
    char* data();

    void update_bounding_box(aku_ParamId param, aku_TimeStamp time);

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

    bool inside_bbox(aku_ParamId param, aku_TimeStamp time) const;

    /**
     * Add new entry to page data.
     * @param entry entry
     * @returns operation status
     */
    int add_entry(aku_ParamId param, aku_TimeStamp timestamp, aku_MemRange data);

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
     * @param index entry index
     * @returns pointer to entry or NULL
     */
    const aku_Entry* read_entry(aku_EntryOffset offset) const;

    /**
     *  Search for entry
     */
    void search(Caller& caller, InternalCursor* cursor, SearchQuery query) const;

    // Only for testing
    void _sort();

    /** Update page index.
      * @param offsets ordered offsets
      * @param num_offsets number of values in buffer
      */
    void sync_next_index(aku_EntryOffset offsets);
};

}  // namespaces
