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
#include <apr_time.h>
#include "sort.h"
#include "page.h"
#include "akumuli_def.h"
#include <boost/lexical_cast.hpp>


namespace Akumuli {

std::ostream& operator << (std::ostream& st, CursorResult res) {
    st << "CursorResult" << boost::to_string(res);
    return st;
}

//------------------------

static SearchQuery::ParamMatch single_param_matcher(aku_ParamId a, aku_ParamId b) {
    if (a == b) {
        return SearchQuery::MATCH;
    }
    return SearchQuery::NO_MATCH;
}

SearchQuery::SearchQuery( aku_ParamId   param_id
                        , aku_TimeStamp low
                        , aku_TimeStamp upp
                        , int           scan_dir)
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(std::bind(&single_param_matcher, param_id, std::placeholders::_1))
    , direction(scan_dir)
{
}

SearchQuery::SearchQuery(MatcherFn matcher
                        , aku_TimeStamp low
                        , aku_TimeStamp upp
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
    : count(count)
    , last_offset(length - 1)
    , sync_index(0)
    , length(length)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , bbox()
{
}

std::pair<aku_EntryOffset, int> PageHeader::index_to_offset(uint32_t index) const {
    if (index < 0 || index > count) {
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

void PageHeader::update_bounding_box(aku_ParamId param, aku_TimeStamp time) {
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

bool PageHeader::inside_bbox(aku_ParamId param, aku_TimeStamp time) const {
    return time  <= bbox.max_timestamp
        && time  >= bbox.min_timestamp
        && param <= bbox.max_id
        && param >= bbox.min_id;
}

void PageHeader::reuse() {
    count = 0;
    open_count++;
    last_offset = length - 1;
    bbox = PageBoundingBox();
}

void PageHeader::close() {
    close_count++;
}

int PageHeader::add_entry(aku_ParamId param, aku_TimeStamp timestamp, aku_MemRange range) {

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

const aku_Entry *PageHeader::read_entry_at(uint32_t index) const {
    if (index >= 0 && index < count) {
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
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, entry_ptr->length);
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

void PageHeader::search(Caller& caller, InternalCursor* cursor, SearchQuery query) const
{
#ifdef DEBUG
    // Debug variables
    aku_TimeStamp dbg_prev_ts;
    long dbg_count = 0;
#endif
    /* Search algorithm outline:
     * - interpolated search for timestamp
     *   - if 5 or more iterations or
     *     search interval is small
     *     BREAK;
     * - binary search for timestamp
     * - scan
     */

    if (!validate_query(query)) {
        cursor->set_error(caller, AKU_SEARCH_EBAD_ARG);
        return;
    }

    bool is_backward = query.direction == AKU_CURSOR_DIR_BACKWARD;
    uint32_t max_index = count - 1u;
    uint32_t begin = 0u;
    uint32_t end = max_index;
    aku_TimeStamp key = is_backward ? query.upperbound
                                    : query.lowerbound;
    uint32_t probe_index = 0u;

    if (key <= bbox.max_timestamp && key >= bbox.min_timestamp) {

        aku_TimeStamp search_lower_bound = bbox.min_timestamp;
        aku_TimeStamp search_upper_bound = bbox.max_timestamp;

        int interpolation_search_quota = 5;

        while(interpolation_search_quota--)  {
            // On small distances - fallback to binary search
            if (end - begin < AKU_INTERPOLATION_SEARCH_CUTOFF)
                break;

            probe_index = ((key - search_lower_bound) * (end - begin)) /
                          (search_upper_bound - search_lower_bound);

            if (probe_index > begin && probe_index < end) {

                auto probe_offset = page_index[probe_index];
                auto probe_entry = read_entry(probe_offset);
                auto probe = probe_entry->time;

                if (probe < key) {
                    begin = probe_index + 1u;
                    probe_offset = page_index[begin];
                    probe_entry = read_entry(probe_offset);
                    search_lower_bound = probe_entry->time;
                } else {
                    end   = probe_index - 1u;
                    probe_offset = page_index[end];
                    probe_entry = read_entry(probe_offset);
                    search_upper_bound = probe_entry->time;
                }
            }
            else {
                break;
                // Continue with binary search
            }
        }
    } else {
        // shortcut for corner cases
        if (key > bbox.max_timestamp) {
            if (is_backward) {
                probe_index = end;
                goto SCAN;
            } else {
                // return empty result
                cursor->complete(caller);
                return;
            }
        }
        else if (key < bbox.min_timestamp) {
            if (!is_backward) {
                probe_index = begin;
                goto SCAN;
            } else {
                // return empty result
                cursor->complete(caller);
                return;
            }
        }
    }
    while (end >= begin) {
        probe_index = begin + ((end - begin) / 2u);
        auto probe_offset = page_index[probe_index];
        auto probe_entry = read_entry(probe_offset);
        auto probe = probe_entry->time;

        if (probe == key) {             // found
            break;
        }
        else if (probe < key) {
            begin = probe_index + 1u;   // change min index to search upper subarray
            if (begin == count)         // we hit the upper bound of the array
                break;
        } else {
            end = probe_index - 1u;     // change max index to search lower subarray
            if (end == ~0u)              // we hit the lower bound of the array
                break;
        }
    }

    // TODO: split this method
SCAN:
    if (is_backward) {
        while (true) {
            auto current_index = probe_index--;
            auto probe_offset = page_index[current_index];
            auto probe_entry = read_entry(probe_offset);
            auto probe = probe_entry->param_id;
            bool probe_in_time_range = query.lowerbound <= probe_entry->time &&
                                       query.upperbound >= probe_entry->time;
            if (query.param_pred(probe) == SearchQuery::MATCH && probe_in_time_range) {
#ifdef DEBUG
                if (dbg_count) {
                    // check for backward direction
                    auto is_ok = dbg_prev_ts >= probe_entry->time;
                    assert(is_ok);
                }
                dbg_prev_ts = probe_entry->time;
                dbg_count++;
#endif
                cursor->put(caller, probe_offset, this);
            }
            if (probe_entry->time < query.lowerbound || current_index == 0u) {
                cursor->complete(caller);
                return;
            }
        }
    } else {
        while (true) {
            auto current_index = probe_index++;
            auto probe_offset = page_index[current_index];
            auto probe_entry = read_entry(probe_offset);
            auto probe = probe_entry->param_id;
            bool probe_in_time_range = query.lowerbound <= probe_entry->time &&
                                       query.upperbound >= probe_entry->time;
            if (query.param_pred(probe) == SearchQuery::MATCH  && probe_in_time_range) {
#ifdef DEBUG
                if (dbg_count) {
                    // check for forward direction
                    auto is_ok = dbg_prev_ts <= probe_entry->time;
                    assert(is_ok);
                }
                dbg_prev_ts = probe_entry->time;
                dbg_count++;
#endif
                cursor->put(caller, probe_offset, this);
            }
            if (probe_entry->time > query.upperbound || current_index == max_index) {
                cursor->complete(caller);
                return;
            }
        }
    }
}

void PageHeader::_sort() {
    auto begin = page_index;
    auto end = page_index + count;
    std::sort(begin, end, [&](aku_EntryOffset a, aku_EntryOffset b) {
        auto ea = read_entry(a);
        auto eb = read_entry(b);
        auto ta = std::tuple<aku_TimeStamp, aku_ParamId>(ea->time, ea->param_id);
        auto tb = std::tuple<aku_TimeStamp, aku_ParamId>(eb->time, eb->param_id);
        return ta < tb;
    });
}

void PageHeader::sync_next_index(aku_EntryOffset offset) {
    if (sync_index == count) {
        AKU_PANIC("sync_index out of range");
    }
    page_index[sync_index++] = offset;
}

}  // namepsace
