/**
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

#include <cstring>
#include <cassert>
#include <algorithm>
#include <mutex>
#include <apr_time.h>
#include "timsort.hpp"
#include "page.h"
#include "compression.h"
#include "akumuli_def.h"
#include "search.h"

#include <random>
#include <iostream>
#include <boost/crc.hpp>


namespace Akumuli {

static SearchQuery::ParamMatch single_param_matcher(aku_ParamId a, aku_ParamId b) {
    if (a == b) {
        return SearchQuery::MATCH;
    }
    return SearchQuery::NO_MATCH;
}

SearchQuery::SearchQuery( aku_ParamId   param_id
                        , aku_Timestamp low
                        , aku_Timestamp upp
                        , int           scan_dir)
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(std::bind(&single_param_matcher, param_id, std::placeholders::_1))
    , direction(scan_dir)
{
}

SearchQuery::SearchQuery(MatcherFn matcher
                        , aku_Timestamp low
                        , aku_Timestamp upp
                        , int scan_dir)
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(matcher)
    , direction(scan_dir)
{
}

// Page
// ----


PageHeader::PageHeader(uint32_t count, uint64_t length, uint32_t page_id)
    : version(0)
    , count(0)
    , next_offset(0)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , length(length - sizeof(PageHeader))
{
}

aku_EntryIndexRecord* PageHeader::page_index(int index) {
    char* ptr = payload + length - sizeof(aku_EntryIndexRecord);
    aku_EntryIndexRecord* entry = reinterpret_cast<aku_EntryIndexRecord*>(ptr);
    entry -= index;
    return entry;
}

const aku_EntryIndexRecord* PageHeader::page_index(int index) const {
    const char* ptr = payload + length - sizeof(aku_EntryIndexRecord);
    const aku_EntryIndexRecord* entry = reinterpret_cast<const aku_EntryIndexRecord*>(ptr);
    entry -= index;
    return entry;
}

std::pair<aku_EntryIndexRecord, int> PageHeader::index_to_offset(uint32_t index) const {
    if (index > count) {
        return std::make_pair(aku_EntryIndexRecord(), AKU_EBAD_ARG);
    }
    return std::make_pair(*page_index(index), AKU_SUCCESS);
}

uint32_t PageHeader::get_entries_count() const {
    return count;
}

size_t PageHeader::get_free_space() const {
    auto begin = payload + next_offset;
    auto end = (payload + length) - count*sizeof(aku_EntryIndexRecord);
    assert(end >= payload);
    return end - begin;
}

void PageHeader::reuse() {
    count = 0;
    checkpoint = 0;
    count = 0;
    open_count++;
    next_offset = 0;
}

void PageHeader::close() {
    close_count++;
}

aku_Status PageHeader::add_entry( const aku_ParamId param
                                , const aku_Timestamp timestamp
                                , const aku_MemRange range )
{
    if (count != 0) {
        // Require >= timestamp
        if (timestamp < page_index(count - 1)->timestamp) {
            return AKU_EBAD_ARG;
        }
    }

    const auto SPACE_REQUIRED = sizeof(aku_Entry)              // entry header
                              + range.length                   // data size (in bytes)
                              + sizeof(aku_EntryIndexRecord);  // offset inside page_index

    const auto ENTRY_SIZE = sizeof(aku_Entry) + range.length;

    if (!range.length) {
        return AKU_WRITE_STATUS_BAD_DATA;
    }
    if (SPACE_REQUIRED > get_free_space()) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    char* free_slot = payload + next_offset;
    aku_Entry* entry = reinterpret_cast<aku_Entry*>(free_slot);
    entry->param_id = param;
    entry->length = range.length;
    memcpy((void*)&entry->value, range.address, range.length);
    page_index(count)->offset = next_offset;
    page_index(count)->timestamp = timestamp;
    next_offset += ENTRY_SIZE;
    count++;
    return AKU_WRITE_STATUS_SUCCESS;
}

int PageHeader::add_chunk(const aku_MemRange range, const uint32_t free_space_required) {
    const auto
        SPACE_REQUIRED = range.length + free_space_required,
        SPACE_NEEDED = range.length;
    if (get_free_space() < SPACE_REQUIRED) {
        return AKU_EOVERFLOW;
    }
    char* free_slot = payload + next_offset;
    memcpy((void*)free_slot, range.address, SPACE_NEEDED);
    next_offset += SPACE_NEEDED;
    return AKU_SUCCESS;
}

int PageHeader::complete_chunk(const ChunkHeader& data) {
    ChunkDesc desc;
    Rand rand;
    aku_Timestamp first_ts;
    aku_Timestamp last_ts;

    struct Writer : ChunkWriter {
        PageHeader *header;
        char* begin;
        char* end;

        Writer(PageHeader *h) : header(h) {}

        virtual aku_MemRange allocate() {
            size_t bytes_free = header->get_free_space();
            char* data = header->payload + header->next_offset;
            begin = data;
            return {(void*)data, (uint32_t)bytes_free};
        }

        virtual aku_Status commit(size_t bytes_written) {
            if (bytes_written < header->get_free_space()) {
                header->next_offset += bytes_written;
                end = begin + bytes_written;
                return AKU_SUCCESS;
            }
            return AKU_EOVERFLOW;
        }
    };
    Writer writer(this);

    // Write compressed data
    aku_Status status = CompressionUtil::encode_chunk(&desc.n_elements, &first_ts, &last_ts, &writer, data);

    // Calculate checksum of the new compressed data
    boost::crc_32_type checksum;

    checksum.process_block(writer.begin, writer.end);
    if (status != AKU_SUCCESS) {
        return status;
    }

    desc.checksum = checksum.checksum();
    desc.begin_offset = writer.begin - payload;
    desc.end_offset = writer.end - payload;

    aku_MemRange head = {&desc, sizeof(desc)};
    status = add_entry(AKU_CHUNK_BWD_ID, first_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    status = add_entry(AKU_CHUNK_FWD_ID, last_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    return status;
}

const aku_Entry *PageHeader::read_entry_at(uint32_t index) const {
    if (index < count) {
        auto offset = page_index(index)->offset;
        return read_entry(offset);
    }
    return 0;
}

const aku_Entry *PageHeader::read_entry(uint32_t offset) const {
    auto ptr = payload + offset;
    auto entry_ptr = reinterpret_cast<const aku_Entry*>(ptr);
    return entry_ptr;
}

const void* PageHeader::read_entry_data(uint32_t offset) const {
    return payload + offset;
}

int PageHeader::get_entry_length_at(int entry_index) const {
    auto entry_ptr = read_entry_at(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::get_entry_length(uint32_t offset) const {
    auto entry_ptr = read_entry(offset);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry_at(int index, aku_Entry *receiver) const {
    auto entry_ptr = read_entry_at(index);
    if (entry_ptr) {
        size_t size = entry_ptr->length + sizeof(aku_Entry);
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, size);
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry(uint32_t offset, aku_Entry *receiver) const {
    auto entry_ptr = read_entry(offset);
    if (entry_ptr) {
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, entry_ptr->length);
        return entry_ptr->length;
    }
    return 0;
}


/** Return false if query is ill-formed.
  * Status and error code fields will be changed accordignly.
  */
static bool validate_query(SearchQuery const& query) {
    // Cursor validation
    if ((query.direction != AKU_CURSOR_DIR_BACKWARD && query.direction != AKU_CURSOR_DIR_FORWARD) ||
         query.upperbound < query.lowerbound)
    {
        return false;
    }
    return true;
}

SearchStats& get_global_search_stats() {
    static SearchStats stats;
    return stats;
}

namespace {
    struct ChunkHeaderSearcher : InterpolationSearch<ChunkHeaderSearcher> {
        ChunkHeader const& header;
        ChunkHeaderSearcher(ChunkHeader const& h) : header(h) {}

        // Interpolation search supporting functions
        bool read_at(aku_Timestamp* out_timestamp, uint32_t ix) const {
            if (ix < header.timestamps.size()) {
                *out_timestamp = header.timestamps[ix];
                return true;
            }
            return false;
        }

        bool is_small(SearchRange range) const {
            return false;
        }

        SearchStats& get_search_stats() {
            return get_global_search_stats();
        }
    };
}

struct SearchAlgorithm : InterpolationSearch<SearchAlgorithm>
{
    PageHeader const* page_;
    Caller& caller_;
    InternalCursor* cursor_;
    SearchQuery query_;

    const uint32_t MAX_INDEX_;
    const bool IS_BACKWARD_;
    const aku_Timestamp key_;

    SearchRange range_;

    SearchAlgorithm(PageHeader const* page, Caller& caller, InternalCursor* cursor, SearchQuery query)
        : page_(page)
        , caller_(caller)
        , cursor_(cursor)
        , query_(query)
        , MAX_INDEX_(page->get_entries_count())
        , IS_BACKWARD_(query.direction == AKU_CURSOR_DIR_BACKWARD)
        , key_(IS_BACKWARD_ ? query.upperbound : query.lowerbound)
    {
        if (MAX_INDEX_) {
            range_.begin = 0u;
            range_.end = MAX_INDEX_ - 1;
        } else {
            range_.begin = 0u;
            range_.end = 0u;
        }
    }

    bool fast_path() {
        if (!MAX_INDEX_) {
            cursor_->complete(caller_);
            return true;
        }

        if (!validate_query(query_)) {
            cursor_->set_error(caller_, AKU_SEARCH_EBAD_ARG);
            return true;
        }

        if (key_ > page_->page_index(range_.end)->timestamp ||
            key_ < page_->page_index(range_.begin)->timestamp)
        {
            // Shortcut for corner cases
            if (key_ > page_->page_index(range_.end)->timestamp) {
                if (IS_BACKWARD_) {
                    range_.begin = range_.end;
                    return false;
                } else {
                    // return empty result
                    cursor_->complete(caller_);
                    return true;
                }
            }
            else if (key_ < page_->page_index(range_.begin)->timestamp) {
                if (!IS_BACKWARD_) {
                    range_.end = range_.begin;
                    return false;
                } else {
                    // return empty result
                    cursor_->complete(caller_);
                    return true;
                }
            }
        }
        return false;
    }

    // Interpolation search supporting functions
    bool read_at(aku_Timestamp* out_timestamp, uint32_t ix) const {
        if (ix < page_->get_entries_count()) {
            *out_timestamp = page_->page_index(ix)->timestamp;
            return true;
        }
        return false;
    }

    bool is_small(SearchRange range) const {
        auto ps = get_page_size();
        auto b = align_to_page(reinterpret_cast<void const*>(page_->read_entry_at(range.begin)), ps);
        auto e = align_to_page(reinterpret_cast<void const*>(page_->read_entry_at(range.end)), ps);
        return b == e;
    }

    SearchStats& get_search_stats() {
        return get_global_search_stats();
    }

    bool interpolation() {
        if (!run(key_, &range_)) {
            cursor_->set_error(caller_, AKU_ENOT_FOUND);
            return false;
        }
        return true;
    }

    void binary_search() {
        // TODO: use binary search from stdlib
        uint64_t steps = 0ul;
        if (range_.begin == range_.end) {
            return;
        }
        uint32_t probe_index = 0u;
        while (range_.end >= range_.begin) {
            steps++;
            probe_index = range_.begin + ((range_.end - range_.begin) / 2u);
            if (probe_index >= MAX_INDEX_) {
                cursor_->set_error(caller_, AKU_EOVERFLOW);
                range_.begin = range_.end = MAX_INDEX_;
                return;
            }

            auto probe = page_->page_index(probe_index)->timestamp;

            if (probe == key_) {                         // found
                break;
            } else if (probe < key_) {
                range_.begin = probe_index + 1u;         // change min index to search upper subarray
                if (range_.begin >= MAX_INDEX_) {        // we hit the upper bound of the array
                    break;
                }
            } else {
                range_.end = probe_index - 1u;           // change max index to search lower subarray
                if (range_.end == ~0u) {                 // we hit the lower bound of the array
                    break;
                }
            }
        }
        range_.begin = probe_index;
        range_.end = probe_index;

        auto& stats = get_global_search_stats();
        std::lock_guard<std::mutex> guard(stats.mutex);
        auto& bst = stats.stats.bstats;
        bst.n_times += 1;
        bst.n_steps += steps;
    }

    bool scan_compressed_entries(aku_Entry const* probe_entry, bool binary_search=false) {
        aku_Status status = AKU_SUCCESS;
        ChunkHeader chunk_header, header;

        auto pdesc  = reinterpret_cast<ChunkDesc const*>(&probe_entry->value[0]);
        auto pbegin = (const unsigned char*)page_->read_entry_data(pdesc->begin_offset);
        auto pend   = (const unsigned char*)page_->read_entry_data(pdesc->end_offset);
        auto probe_length = pdesc->n_elements;

        // TODO:checksum!
        boost::crc_32_type checksum;
        checksum.process_block(pbegin, pend);
        if (checksum.checksum() != pdesc->checksum) {
            AKU_PANIC("File damaged!");
            // TODO: report error
            return false;
        }

        status = CompressionUtil::decode_chunk(&chunk_header, pbegin, pend, probe_length);
        if (status != AKU_SUCCESS) {
            AKU_PANIC("Can't decode chunk");
        }

        // TODO: depending on a query type we can use chunk order or convert back to time-order.
        // If we extract evertyhing it is better to convert to time order. If we picking some
        // parameter ids it is better to check if this ids present in a chunk and extract values
        // in chunk order and only after that - convert results to time-order.

        // Convert from chunk order to time order
        if (!CompressionUtil::convert_from_chunk_order(chunk_header, &header)) {
            AKU_PANIC("Bad chunk");
        }

        int start_pos = 0;
        if (IS_BACKWARD_) {
            start_pos = static_cast<int>(probe_length - 1);
        }
        bool probe_in_time_range = true;

        auto cursor = cursor_;
        auto& caller = caller_;
        auto page = page_;

        auto put_entry = [&header, cursor, &caller, page] (uint32_t i) {
            aku_PData pdata;
            if (header.values.at(i).type == ChunkValue::BLOB) {
                pdata.type =  aku_PData::BLOB;
                pdata.value.blob.begin = page->read_entry_data(header.values.at(i).value.blobval.offset);
                pdata.value.blob.size = header.values.at(i).value.blobval.length;
            } else if (header.values.at(i).type == ChunkValue::FLOAT) {
                pdata.type = aku_PData::FLOAT;
                pdata.value.float64 = header.values.at(i).value.floatval;
            }
            CursorResult result = {
                header.timestamps.at(i),
                header.paramids.at(i),
                pdata,
            };
            cursor->put(caller, result);
        };

        if (IS_BACKWARD_) {
            for (int i = static_cast<int>(start_pos); i >= 0; i--) {
                probe_in_time_range = query_.lowerbound <= header.timestamps[i] &&
                                      query_.upperbound >= header.timestamps[i];
                if (probe_in_time_range && query_.param_pred(header.paramids[i]) == SearchQuery::MATCH) {
                    put_entry(i);
                } else {
                    probe_in_time_range = query_.lowerbound <= header.timestamps[i];
                    if (!probe_in_time_range) {
                        break;
                    }
                }
            }
        } else {
            for (auto i = start_pos; i != (int)probe_length; i++) {
                probe_in_time_range = query_.lowerbound <= header.timestamps[i] &&
                                      query_.upperbound >= header.timestamps[i];
                if (probe_in_time_range && query_.param_pred(header.paramids[i]) == SearchQuery::MATCH) {
                    put_entry(i);
                } else {
                    probe_in_time_range = query_.upperbound >= header.timestamps[i];
                    if (!probe_in_time_range) {
                        break;
                    }
                }
            }
        }
        return probe_in_time_range;
    }

    std::tuple<uint64_t, uint64_t> scan_impl(uint32_t probe_index) {
#ifdef DEBUG
        // Debug variables
        aku_Timestamp dbg_prev_ts;
        long dbg_count = 0;
#endif
        int index_increment = IS_BACKWARD_ ? -1 : 1;
        while (true) {
            auto current_index = probe_index;
            probe_index += index_increment;
            auto probe_offset = page_->page_index(current_index)->offset;
            auto probe_time = page_->page_index(current_index)->timestamp;
            auto probe_entry = page_->read_entry(probe_offset);
            auto probe = probe_entry->param_id;
            bool proceed = false;
            bool probe_in_time_range = query_.lowerbound <= probe_time &&
                                       query_.upperbound >= probe_time;
            if (probe < AKU_ID_COMPRESSED) {
                if (query_.param_pred(probe) == SearchQuery::MATCH && probe_in_time_range) {
#ifdef DEBUG
                    if (dbg_count) {
                        // check for backward direction
                        auto is_ok = IS_BACKWARD_ ? (dbg_prev_ts >= probe_time)
                                                  : (dbg_prev_ts <= probe_time);
                        assert(is_ok);
                    }
                    dbg_prev_ts = probe_time;
                    dbg_count++;
#endif
                    aku_PData pdata;
                    pdata.type = aku_PData::BLOB;
                    pdata.value.blob.begin = page_->read_entry_data(probe_offset + sizeof(aku_Entry));
                    pdata.value.blob.size = probe_entry->length;
                    CursorResult result = {
                        probe_time,
                        probe,  //id
                        pdata
                    };
                    if (!cursor_->put(caller_, result)) {
                        break;
                    }
                }
                proceed = IS_BACKWARD_ ? query_.lowerbound <= probe_time
                                       : query_.upperbound >= probe_time;
            } else {
                if (probe == AKU_CHUNK_FWD_ID && IS_BACKWARD_ == false) {
                    proceed = scan_compressed_entries(probe_entry, false);
                } else if (probe == AKU_CHUNK_BWD_ID && IS_BACKWARD_ == true) {
                    proceed = scan_compressed_entries(probe_entry, false);
                } else {
                    proceed = IS_BACKWARD_ ? query_.lowerbound <= probe_time
                                           : query_.upperbound >= probe_time;
                }
            }
            if (!proceed || probe_index >= MAX_INDEX_) {
                // When scanning forward probe_index will be equal to MAX_INDEX_ at the end of the page
                // When scanning backward probe_index will be equal to ~0 (probe_index > MAX_INDEX_)
                // at the end of the page
                break;
            }
        }
        return std::make_tuple(0ul, 0ul);
    }

    void scan() {
        if (range_.begin != range_.end) {
            cursor_->set_error(caller_, AKU_EGENERAL);
            return;
        }
        if (range_.begin >= MAX_INDEX_) {
            cursor_->set_error(caller_, AKU_EOVERFLOW);
            return;
        }

        auto sums = scan_impl(range_.begin);

        auto& stats = get_global_search_stats();
        {
            std::lock_guard<std::mutex> guard(stats.mutex);
            stats.stats.scan.fwd_bytes += std::get<0>(sums);
            stats.stats.scan.bwd_bytes += std::get<1>(sums);
        }
        cursor_->complete(caller_);
    }
};

void PageHeader::search(Caller& caller, InternalCursor* cursor, SearchQuery query) const
{
    SearchAlgorithm search_alg(this, caller, cursor, query);
    if (search_alg.fast_path() == false) {
        if (search_alg.interpolation()) {
            search_alg.binary_search();
            search_alg.scan();
        }
    }
}

void PageHeader::get_search_stats(aku_SearchStats* stats, bool reset) {
    auto& gstats = get_global_search_stats();
    std::lock_guard<std::mutex> guard(gstats.mutex);

    memcpy( reinterpret_cast<void*>(stats)
          , reinterpret_cast<void*>(&gstats.stats)
          , sizeof(aku_SearchStats));

    if (reset) {
        memset(reinterpret_cast<void*>(&gstats.stats), 0, sizeof(aku_SearchStats));
    }
}

}  // namepsace
