/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
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

//---------------Timestamp

TimeStamp TimeStamp::utc_now() noexcept {
    int64_t t = apr_time_now();
    return { t };
}

bool TimeStamp::operator  < (TimeStamp other) const noexcept {
    return value < other.value;
}

bool TimeStamp::operator  > (TimeStamp other) const noexcept {
    return value > other.value;
}

bool TimeStamp::operator == (TimeStamp other) const noexcept {
    return value == other.value;
}

bool TimeStamp::operator <= (TimeStamp other) const noexcept {
    return value <= other.value;
}

bool TimeStamp::operator >= (TimeStamp other) const noexcept {
    return value >= other.value;
}

TimeDuration TimeStamp::operator - (TimeStamp other) const noexcept {
    return { value - other.value };
}

const TimeStamp TimeStamp::MAX_TIMESTAMP = {std::numeric_limits<int64_t>::max()};

const TimeStamp TimeStamp::MIN_TIMESTAMP = {0L};

//------------------------

Entry::Entry(uint32_t length)
    : param_id {}
    , time {}
    , length(length)
{
}

Entry::Entry(uint32_t param_id, TimeStamp timestamp, uint32_t length)
    : param_id(param_id)
    , time(timestamp)
    , length(length)
{
}

uint32_t Entry::get_size(uint32_t load_size) noexcept {
    return sizeof(Entry) - sizeof(uint32_t) + load_size;
}

aku_MemRange Entry::get_storage() const noexcept {
    return { (void*)value, length };
}


Entry2::Entry2(uint32_t param_id, TimeStamp time, aku_MemRange range)
    : param_id(param_id)
    , time(time)
    , range(range)
{
}



// Cursors
// -------

static SearchQuery::ParamMatch single_param_matcher(ParamId a, ParamId b) {
    if (a == b) {
        return SearchQuery::MATCH;
    }
    return SearchQuery::NO_MATCH;
}

SearchQuery::SearchQuery( ParamId      param_id
                        , TimeStamp    low
                        , TimeStamp    upp
                        , uint32_t     scan_dir)  noexcept
    : lowerbound(low)
    , upperbound(upp)
    , param_pred(std::bind(&single_param_matcher, param_id, std::placeholders::_1))
    , direction(scan_dir)
{
}

SearchQuery::SearchQuery( MatcherFn matcher
                        , TimeStamp    low
                        , TimeStamp    upp
                        , uint32_t     scan_dir)  noexcept
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
    max_timestamp = TimeStamp::MIN_TIMESTAMP;
    min_timestamp = TimeStamp::MAX_TIMESTAMP;
}


const char* PageHeader::cdata() const noexcept {
    return reinterpret_cast<const char*>(this);
}

char* PageHeader::data() noexcept {
    return reinterpret_cast<char*>(this);
}

PageHeader::PageHeader(PageType type, uint32_t count, uint64_t length, uint32_t page_id)
    : type(type)
    , count(count)
    , last_offset(length - 1)
    , sync_index(0)
    , length(length)
    , open_count(0)
    , close_count(0)
    , page_id(page_id)
    , bbox()
{
}

std::pair<EntryOffset, int> PageHeader::index_to_offset(uint32_t index) const noexcept {
    if (index < 0 || index > count) {
        return std::make_pair(0u, AKU_EBAD_ARG);
    }
    return std::make_pair(page_index[index], AKU_SUCCESS);
}

int PageHeader::get_entries_count() const noexcept {
    return (int)this->count;
}

size_t PageHeader::get_free_space() const noexcept {
    auto begin = reinterpret_cast<const char*>(page_index + count);
    const char* end = cdata();
    end += last_offset;
    assert(end >= begin);
    return end - begin;
}

void PageHeader::update_bounding_box(ParamId param, TimeStamp time) noexcept {
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

bool PageHeader::inside_bbox(ParamId param, TimeStamp time) const noexcept {
    return time  <= bbox.max_timestamp
        && time  >= bbox.min_timestamp
        && param <= bbox.max_id
        && param >= bbox.min_id;
}

void PageHeader::reuse() noexcept {
    count = 0;
    open_count++;
    last_offset = length - 1;
    bbox = PageBoundingBox();
}

void PageHeader::close() noexcept {
    close_count++;
}

int PageHeader::add_entry(Entry const& entry) noexcept {
    auto space_required = entry.length + sizeof(EntryOffset);
    if (entry.length < sizeof(Entry)) {
        return AKU_WRITE_STATUS_BAD_DATA;
    }
    if (space_required > get_free_space()) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    char* free_slot = data() + last_offset;
    free_slot -= entry.length;
    memcpy((void*)free_slot, (void*)&entry, entry.length);
    last_offset = free_slot - cdata();
    page_index[count] = last_offset;
    count++;
    // FIXME: split param_id update
    update_bounding_box(entry.param_id, entry.time);
    return AKU_WRITE_STATUS_SUCCESS;
}

int PageHeader::add_entry(Entry2 const& entry) noexcept {

    static const int ENTRY_LEN = sizeof(TimeStamp) + sizeof(ParamId);

    auto space_required = entry.range.length
                        + sizeof(uint32_t)
                        + ENTRY_LEN
                        + sizeof(EntryOffset);

    auto free_space = get_free_space();
    if (space_required > free_space) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }

    char* free_slot = data() + last_offset;
    char* orig_free_slot = free_slot;
    // FIXME: reorder to improve memory performance
    // Write data
    free_slot -= entry.range.length;
    memcpy((void*)free_slot, entry.range.address, entry.range.length);
    assert(free_slot > reinterpret_cast<char*>(page_index + count));
    // Write length
    free_slot -= sizeof(uint32_t);
    *(uint32_t*)free_slot = entry.range.length;
    // Write paramId and timestamp
    free_slot -= ENTRY_LEN;
    memcpy((void*)free_slot, (void*)&entry, ENTRY_LEN);
    last_offset = free_slot - cdata();
    page_index[count] = last_offset;
    count++;
    update_bounding_box(entry.param_id, entry.time);
    assert(orig_free_slot == (free_slot + space_required - sizeof(uint32_t)));
    return AKU_WRITE_STATUS_SUCCESS;
}

const Entry* PageHeader::read_entry_at(uint32_t index) const noexcept {
    if (index >= 0 && index < count) {
        auto offset = page_index[index];
        return read_entry(offset);
    }
    return 0;
}

const Entry* PageHeader::read_entry(EntryOffset offset) const noexcept {
    auto ptr = cdata() + offset;
    auto entry_ptr = reinterpret_cast<const Entry*>(ptr);
    return entry_ptr;
}

int PageHeader::get_entry_length_at(int entry_index) const noexcept {
    auto entry_ptr = read_entry_at(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::get_entry_length(EntryOffset offset) const noexcept {
    auto entry_ptr = read_entry(offset);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry_at(int index, Entry* receiver) const noexcept {
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

int PageHeader::copy_entry(EntryOffset offset, Entry* receiver) const noexcept {
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
static bool validate_query(SearchQuery const& query) noexcept {
    // Cursor validation
    if ((query.direction != AKU_CURSOR_DIR_BACKWARD && query.direction != AKU_CURSOR_DIR_FORWARD) ||
         query.upperbound < query.lowerbound)
    {
        return false;
    }
    return true;
}


void PageHeader::search(Caller& caller, InternalCursor* cursor, SearchQuery const &query) const noexcept
{
#ifdef DEBUG
    // Debug variables
    TimeStamp dbg_prev_ts;
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
    int64_t key = is_backward ? query.upperbound.value
                              : query.lowerbound.value;
    uint32_t probe_index = 0u;

    if (key <= bbox.max_timestamp.value && key >= bbox.min_timestamp.value) {

        int64_t search_lower_bound = bbox.min_timestamp.value;
        int64_t search_upper_bound = bbox.max_timestamp.value;

        int interpolation_search_quota = 5;

        while(interpolation_search_quota--)  {
            // On small distances - fallback to binary search
            if (end - begin < AKU_INTERPOLATION_SEARCH_CUTOFF)
                break;

            probe_index = ((key - search_lower_bound) * (end - begin)) /
                          (search_upper_bound - search_lower_bound);

            if (probe_index > begin && probe_index < end) {

                auto probe_offset = page_index[probe_index];
                auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                auto probe = probe_entry->time.value;

                if (probe < key) {
                    begin = probe_index + 1u;
                    probe_offset = page_index[begin];
                    probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                    search_lower_bound = probe_entry->time.value;
                } else {
                    end   = probe_index - 1u;
                    probe_offset = page_index[end];
                    probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                    search_upper_bound = probe_entry->time.value;
                }
            }
            else {
                break;
                // Continue with binary search
            }
        }
    } else {
        // shortcut for corner cases
        if (key > bbox.max_timestamp.value) {
            if (is_backward) {
                probe_index = end;
                goto SCAN;
            } else {
                // return empty result
                cursor->complete(caller);
                return;
            }
        }
        else if (key < bbox.min_timestamp.value) {
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
        auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
        auto probe = probe_entry->time.value;

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
            auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
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
            auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
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

void PageHeader::_sort() noexcept {
    auto begin = page_index;
    auto end = page_index + count;
    std::sort(begin, end, [&](EntryOffset a, EntryOffset b) {
        auto ea = reinterpret_cast<const Entry*>(cdata() + a);
        auto eb = reinterpret_cast<const Entry*>(cdata() + b);
        auto ta = std::tuple<uint64_t, uint32_t>(ea->time.value, ea->param_id);
        auto tb = std::tuple<uint64_t, uint32_t>(eb->time.value, eb->param_id);
        return ta < tb;
    });
}

void PageHeader::sync_next_index(EntryOffset offset) noexcept {
    if (sync_index == count) {
        AKU_PANIC("sync_index out of range");
    }
    page_index[sync_index++] = offset;
}

}  // namepsace
