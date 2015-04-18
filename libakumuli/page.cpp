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

PageBoundingBox::PageBoundingBox()
    : max_id(0)
    , min_id(std::numeric_limits<uint32_t>::max())
{
    max_timestamp = AKU_MIN_TIMESTAMP;
    min_timestamp = AKU_MAX_TIMESTAMP;
}


const char* PageHeader::cdata() const {
    return reinterpret_cast<const char*>(this);
}

char* PageHeader::data() {
    return reinterpret_cast<char*>(this);
}

PageHeader::PageHeader(uint32_t count, uint64_t length, uint32_t page_id)
    : version(0)
    , count(count)
    , last_offset(length - 1)
    , sync_count(0)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , length(length)
    , bbox()
{
    // zero out histogram
    memset(&histogram, 0, sizeof(histogram));
}

std::pair<aku_EntryOffset, int> PageHeader::index_to_offset(uint32_t index) const {
    if (index > count) {
        return std::make_pair(0u, AKU_EBAD_ARG);
    }
    return std::make_pair(page_index[index], AKU_SUCCESS);
}

int PageHeader::get_entries_count() const {
    return (int)this->count;
}

size_t PageHeader::get_free_space() const {
    auto begin = reinterpret_cast<const char*>(page_index + count);
    const char* end = cdata();
    end += last_offset;
    assert(end >= begin);
    return end - begin;
}

void PageHeader::update_bounding_box(aku_ParamId param, aku_Timestamp time) {
    if (param > bbox.max_id) {
        bbox.max_id = param;
    }
    if (param < bbox.min_id) {
        bbox.min_id = param;
    }
    if (time > bbox.max_timestamp) {
        bbox.max_timestamp = time;
    }
    if (time < bbox.min_timestamp) {
        bbox.min_timestamp = time;
    }
}

bool PageHeader::inside_bbox(aku_ParamId param, aku_Timestamp time) const {
    return time  <= bbox.max_timestamp
        && time  >= bbox.min_timestamp
        && param <= bbox.max_id
        && param >= bbox.min_id;
}

void PageHeader::reuse() {
    sync_count = 0;
    checkpoint = 0;
    count = 0;
    open_count++;
    last_offset = length - 1;
    bbox = PageBoundingBox();
    histogram.size = 0;
}

void PageHeader::close() {
    close_count++;
}

int PageHeader::add_entry( const aku_ParamId param
                         , const aku_Timestamp timestamp
                         , const aku_MemRange range )
{

    const auto SPACE_REQUIRED = sizeof(aku_Entry)         // entry header
                              + range.length              // data size (in bytes)
                              + sizeof(aku_EntryOffset);  // offset inside page_index

    const auto ENTRY_SIZE = sizeof(aku_Entry) + range.length;

    if (!range.length) {
        return AKU_WRITE_STATUS_BAD_DATA;
    }
    if (SPACE_REQUIRED > get_free_space()) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    char* free_slot = data() + last_offset;
    free_slot -= ENTRY_SIZE;
    aku_Entry* entry = reinterpret_cast<aku_Entry*>(free_slot);
    entry->param_id = param;
    entry->time = timestamp;
    entry->length = range.length;
    memcpy((void*)entry->value, range.address, range.length);
    last_offset = free_slot - cdata();
    page_index[count] = last_offset;
    count++;
    update_bounding_box(param, timestamp);
    return AKU_WRITE_STATUS_SUCCESS;
}

int PageHeader::add_chunk(const aku_MemRange range, const uint32_t free_space_required) {
    const auto
        SPACE_REQUIRED = range.length + free_space_required,
        SPACE_NEEDED = range.length;
    if (get_free_space() < SPACE_REQUIRED) {
        return AKU_EOVERFLOW;
    }
    char* free_slot = data() + last_offset;
    free_slot -= SPACE_NEEDED;
    memcpy((void*)free_slot, range.address, SPACE_NEEDED);
    last_offset = free_slot - cdata();
    return AKU_SUCCESS;
}

int PageHeader::complete_chunk(const ChunkHeader& data) {
    ChunkDesc desc;
    Rand rand;
    aku_Timestamp first_ts;
    aku_Timestamp last_ts;
    struct Writer : ChunkWriter {
        PageHeader *header;
        Writer(PageHeader *h) : header(h) {}
        virtual aku_Status add_chunk(aku_MemRange range, size_t size_estimate) {
            return header->add_chunk(range, size_estimate);
        }
    } writer(this);

    // Write compressed data
    aku_Status status = CompressionUtil::encode_chunk(&desc.n_elements, &first_ts, &last_ts, &writer, data);

    // Calculate checksum of the new compressed data
    boost::crc_32_type checksum;
    uint32_t end = 0u;
    uint32_t begin = last_offset;
    if (count == 0) {
        // This is a first chunk!
        end = length - 1;
    } else {
        end = page_index[count-1];
    }
    checksum.process_block(cdata() + begin, cdata() + end);
    if (status != AKU_SUCCESS) {
        return status;
    }
    desc.checksum = checksum.checksum();
    desc.begin_offset = begin;
    desc.end_offset = end;
    aku_MemRange head;
    head = {&desc, sizeof(desc)};
    status = add_entry(AKU_CHUNK_BWD_ID, first_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    sync_next_index(last_offset, rand(), false);
    status = add_entry(AKU_CHUNK_FWD_ID, last_ts, head);
    if (status != AKU_SUCCESS) {
        return status;
    }
    sync_next_index(last_offset, rand(), false);
    // Sort histogram
    sync_next_index(0, 0, true);
    return status;
}

const aku_Entry *PageHeader::read_entry_at(uint32_t index) const {
    if (index < count) {
        auto offset = page_index[index];
        return read_entry(offset);
    }
    return 0;
}

const aku_Entry *PageHeader::read_entry(aku_EntryOffset offset) const {
    auto ptr = cdata() + offset;
    auto entry_ptr = reinterpret_cast<const aku_Entry*>(ptr);
    return entry_ptr;
}

const void* PageHeader::read_entry_data(aku_EntryOffset offset) const {
    return cdata() + offset;
}

int PageHeader::get_entry_length_at(int entry_index) const {
    auto entry_ptr = read_entry_at(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::get_entry_length(aku_EntryOffset offset) const {
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

int PageHeader::copy_entry(aku_EntryOffset offset, aku_Entry *receiver) const {
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
        , MAX_INDEX_(page->sync_count)
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

        if (key_ > page_->bbox.max_timestamp || key_ < page_->bbox.min_timestamp) {
            // Shortcut for corner cases
            if (key_ > page_->bbox.max_timestamp) {
                if (IS_BACKWARD_) {
                    range_.begin = range_.end;
                    return false;
                } else {
                    // return empty result
                    cursor_->complete(caller_);
                    return true;
                }
            }
            else if (key_ < page_->bbox.min_timestamp) {
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

    void histogram() {
        auto const& h = page_->histogram;
        auto pred = [](PageHistogramEntry const& a, PageHistogramEntry const& b) {
            return a.timestamp < b.timestamp;
        };
        PageHistogramEntry hkey = { key_, 0 };
        auto begin = h.entries;
        auto end = begin + h.size;
        auto upper = std::upper_bound(begin, end, hkey, pred);
        auto lower = std::lower_bound(begin, end, hkey, pred);
        if (lower != end) {
            if (lower != begin && lower->timestamp > hkey.timestamp) {
                lower--;
            }
            range_.begin = lower->index;
        }
        if (upper != end) {
            range_.end = upper->index;
        }
    }

    // Interpolation search supporting functions
    bool read_at(aku_Timestamp* out_timestamp, uint32_t ix) const {
        const aku_Entry *entry = page_->read_entry_at(ix);
        if (entry) {
            *out_timestamp = entry->time;
        }
        return entry != nullptr;
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
            auto probe_offset = page_->page_index[probe_index];
            auto probe_entry = page_->read_entry(probe_offset);
            auto probe = probe_entry->time;

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

    bool scan_compressed_entries(aku_Entry const* probe_entry, bool binary_search=false)
    {
        ChunkHeader chunk_header, header;

        auto pdesc = reinterpret_cast<ChunkDesc const*>(&probe_entry->value[0]);
        auto pbegin = (const unsigned char*)(page_->cdata() + pdesc->begin_offset);
        auto pend = (const unsigned char*)(page_->cdata() + pdesc->end_offset);
        auto probe_length = pdesc->n_elements;

        // TODO:checksum!
        boost::crc_32_type checksum;
        checksum.process_block(pbegin, pend);
        if (checksum.checksum() != pdesc->checksum) {
            AKU_PANIC("File damaged!");
            // TODO: report error
            return false;
        }

        CompressionUtil::decode_chunk(&chunk_header, &pbegin, pend, 0, 5, probe_length);

        // TODO: depending on a query type we can use chunk order or convert back to time-order.
        // If we extract evertyhing it is better to convert to time order. If we picking some
        // parameter ids it is better to check if this ids present in a chunk and extract values
        // in chunk order and only after that - convert results to time-order.

        // Convert from chunk order to time order
        if (!CompressionUtil::convert_from_chunk_order(chunk_header, &header)) {
            AKU_PANIC("Bad chunk");
        }

        int start_pos = 0;
        int value_start_pos = 0;
        if (IS_BACKWARD_) {
            start_pos = static_cast<int>(probe_length - 1);
            value_start_pos = static_cast<int>(chunk_header.values.size() - 1);
        }
        bool probe_in_time_range = true;

        auto cursor = cursor_;
        auto& caller = caller_;
        auto page = page_;
        auto ix_value = IS_BACKWARD_ ? value_start_pos : 0;
        auto put_entry = [&header, cursor, &caller, page, &ix_value] (uint32_t i) {
            auto len = header.lengths[i];
            CursorResult result = {
                len,
                header.timestamps[i],
                header.paramids[i],
            };
            if (len == 0) {
                result.data.float64 = header.values.at(ix_value);
            } else {
                result.data.ptr = page->read_entry_data(header.offsets.at(i));
            }
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
                if (header.lengths.at(i) == 0) {
                    ix_value--;
                }
            }
        } else {
            // TODO: limit chunk size
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
                if (header.lengths.at(i) == 0) {
                    ix_value++;
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
            auto probe_offset = page_->page_index[current_index];
            auto probe_entry = page_->read_entry(probe_offset);
            auto probe = probe_entry->param_id;
            bool proceed = false;
            bool probe_in_time_range = query_.lowerbound <= probe_entry->time &&
                                       query_.upperbound >= probe_entry->time;
            if (probe < AKU_ID_COMPRESSED) {
                if (query_.param_pred(probe) == SearchQuery::MATCH && probe_in_time_range) {
#ifdef DEBUG
                    if (dbg_count) {
                        // check for backward direction
                        auto is_ok = IS_BACKWARD_ ? (dbg_prev_ts >= probe_entry->time)
                                                  : (dbg_prev_ts <= probe_entry->time);
                        assert(is_ok);
                    }
                    dbg_prev_ts = probe_entry->time;
                    dbg_count++;
#endif
                    auto offset = static_cast<aku_EntryOffset>(probe_offset + sizeof(aku_Entry));
                    CursorResult result = {
                        probe_entry->length,
                        probe_entry->time,
                        probe,//id
                        page_->read_entry_data(offset)
                    };
                    if (!cursor_->put(caller_, result)) {
                        break;
                    }
                }
                proceed = IS_BACKWARD_ ? query_.lowerbound <= probe_entry->time
                                       : query_.upperbound >= probe_entry->time;
            } else {
                if (probe == AKU_CHUNK_FWD_ID && IS_BACKWARD_ == false) {
                    proceed = scan_compressed_entries(probe_entry, false);
                } else if (probe == AKU_CHUNK_BWD_ID && IS_BACKWARD_ == true) {
                    proceed = scan_compressed_entries(probe_entry, false);
                } else {
                    proceed = IS_BACKWARD_ ? query_.lowerbound <= probe_entry->time
                                           : query_.upperbound >= probe_entry->time;
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
        search_alg.histogram();
        if (search_alg.interpolation()) {
            search_alg.binary_search();
            search_alg.scan();
        }
    }
}

void PageHeader::_sort() {
    // This method is only for testing purposes.
    // Page invariants can break here.
    auto begin = page_index + sync_count;
    auto end = page_index + count;
    std::sort(begin, end, [&](aku_EntryOffset a, aku_EntryOffset b) {
        auto ea = read_entry(a);
        auto eb = read_entry(b);
        auto ta = std::tuple<aku_Timestamp, aku_ParamId>(ea->time, ea->param_id);
        auto tb = std::tuple<aku_Timestamp, aku_ParamId>(eb->time, eb->param_id);
        return ta < tb;
    });
    sync_count = count;
}

void PageHeader::sync_next_index(aku_EntryOffset offset, uint32_t rand_val, bool sort_histogram) {
    // sync_count updated only here! 
    if (!sort_histogram) {
        if (sync_count >= count) {
            AKU_PANIC("sync_index out of range");
        }
        auto index = sync_count++;
        page_index[index] = offset;

        if (histogram.size < AKU_HISTOGRAM_SIZE) {
            // first AKU_HISTOGRAM_SIZE samples
            auto& h = histogram.entries[histogram.size++];
            h.index = index;
            h.timestamp = read_entry(offset)->time;
        } else {
            // reservoir sampling
            auto rindex = static_cast<uint32_t>(rand_val % sync_count);
            if (rindex < histogram.size) {
                auto& h = histogram.entries[rindex];
                h.index = index;
                h.timestamp = read_entry(offset)->time;
            }
        }
    } else {
        gfx::timsort(histogram.entries, histogram.entries + histogram.size,
                  [](PageHistogramEntry const& a, PageHistogramEntry const& b) {
                        return a.timestamp < b.timestamp;
                  }
        );
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
