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
#include "akumuli.h"
#include "buffer_cache.h"
#include "storage_engine/compression.h"
#include "internal_cursor.h"
#include "queryprocessor_framework.h"
#include "util.h"
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>

const int64_t AKU_MAX_PAGE_SIZE = 0x100000000;

namespace Akumuli {

typedef uint64_t aku_Duration;  //< Time duration

//! Entry index record
struct aku_EntryIndexRecord {
    aku_Timestamp timestamp;
    uint32_t      offset;
} __attribute__((packed));

struct CompressedChunkDesc {
    uint32_t n_elements;    //< Number of elements in a chunk
    uint32_t begin_offset;  //< Data begin offset
    uint32_t end_offset;    //< Data end offset
    uint32_t checksum;      //< Checksum
} __attribute__((packed));


struct aku_Entry {
    //aku_Timestamp  time;      //< Entry timestamp
    aku_ParamId param_id;  //< Parameter ID
    uint32_t    length;    //< Entry length: constant + variable sized parts
    uint32_t    value[];   //< Data begining
} __attribute__((packed));

//! PageHeader forward declaration
struct PageHeader;

/** Page bounding box.
 *  All data is two dimentional: param-timestamp.
 */

struct SearchStats {
    aku_SearchStats stats;
    std::mutex      mutex;

    SearchStats() { memset(&stats, 0, sizeof(stats)); }
};

SearchStats& get_global_search_stats();


/**
 * In-memory page representation.
 * PageHeader represents begining of the page.
 * Entry indexes grows from low to high adresses.
 * Entries placed in the bottom of the page.
 * This class must be nonvirtual.
 */
class PageHeader {
    // metadata
    const uint32_t version;      //< format version
    uint32_t       count;        //< number of elements stored
    uint32_t       next_offset;  //< offset of the last added record in payload array
    uint32_t       checkpoint;   //< page checkpoint index
    uint32_t       open_count;   //< how many times page was open for write
    uint32_t       close_count;  //< how many times page was closed for write
    uint32_t       page_id;      //< page index in storage
    uint32_t       numpages;     //< total number or pages
    uint64_t       length;       //< payload size
    char           payload[];    //< page payload

public:
    //! Get length of the page
    uint64_t get_page_length() const;

    //! Get page ID
    uint32_t get_page_id() const;

    //! Get number of pages
    uint32_t get_numpages() const;

    //! Number of times page was opened for writing
    uint32_t get_open_count() const;

    //! Number of times page was closed for writing
    uint32_t get_close_count() const;

    //! Set open count
    void set_open_count(uint32_t cnt);

    //! Set open count
    void set_close_count(uint32_t cnt);

    ///     Checkpoint and restore      ///

    //! Create checkpoint. Flush should be performed twice, before and after call to this method
    void create_checkpoint();

    //! Restore, return true if flush needed
    bool restore();


    //! Convert entry index to entry offset
    std::pair<aku_EntryIndexRecord, int> index_to_offset(uint32_t index) const;

    aku_EntryIndexRecord* page_index(int index);

    const aku_EntryIndexRecord* page_index(int index) const;

    //! C-tor
    PageHeader(uint32_t count, uint64_t length, uint32_t page_id, uint32_t numpages);

    //! Clear all page conent (open_count += 1)
    void reuse();

    //! Close page for write (close_count += 1)
    void close();

    //! Return number of entries stored in page
    uint32_t get_entries_count() const;

    //! Returns amount of free space in bytes
    size_t get_free_space() const;

    bool inside_bbox(aku_ParamId param, aku_Timestamp time) const;

    /**
     * Add new entry to page data.
     * @param entry entry
     * @returns operation status
     */
    aku_Status add_entry(const aku_ParamId param, const aku_Timestamp timestamp,
                         const aku_MemRange& range);

    /**
     * Add some data to last entry. (without length)
     * @param data data element
     * @param free_space_required minimum amount of space inside the page
     * @returns operation status
     */
    aku_Status add_chunk(const aku_MemRange data, const uint32_t free_space_required,
                         uint32_t* out_offset);

    /**
     * Complete chunk. Add compressed header and index.
     * @param data chunk header data (list of sorted timestamps, param ids, offsets and lengths
     * @returns operation status
     */
    aku_Status complete_chunk(const UncompressedChunk& data);

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
    int get_entry_length(uint32_t offset) const;

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
    int copy_entry(uint32_t offset, aku_Entry* receiver) const;

    /**
     * Get pointer to entry without copying using index
     * @param index entry index
     * @returns pointer to entry or NULL
     */
    const aku_Entry* read_entry_at(uint32_t index) const;

    /**
     * Get entry timefstamp by index
     * @param index entry index
     * @return timestamp
     */
    const aku_Timestamp read_timestamp_at(uint32_t index) const;

    /**
     * Get pointer to entry without copying using offset
     * @param entry offset
     * @returns pointer to entry or NULL
     */
    const aku_Entry* read_entry(uint32_t offset) const;

    /**
     * Get pointer to entry data without copying using
     * data offset.
     * @param data offset
     * @returns pointer to entry data or NULL
     */
    const void* read_entry_data(uint32_t offset) const;

    /**
      * @brief Search matches inside the volume
      */
    void search(std::shared_ptr<QP::IQueryProcessor> query,
                std::shared_ptr<ChunkCache>          cache = std::shared_ptr<ChunkCache>()) const;

    static void get_search_stats(aku_SearchStats* stats, bool reset = false);

    //! Get page status
    void get_stats(aku_StorageStats* rcv_stats);
};

}  // namespaces
