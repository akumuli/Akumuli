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
#include "timsort.hpp"
#include "page.h"
#include "akumuli_def.h"


namespace Akumuli {

//---------------Timestamp

TimeStamp TimeStamp::utc_now() noexcept {
    int64_t t = apr_time_now();
    return { t };
}

bool TimeStamp::operator  < (TimeStamp other) const noexcept {
    return precise < other.precise;
}

bool TimeStamp::operator  > (TimeStamp other) const noexcept {
    return precise > other.precise;
}

bool TimeStamp::operator == (TimeStamp other) const noexcept {
    return precise == other.precise;
}

bool TimeStamp::operator <= (TimeStamp other) const noexcept {
    return precise <= other.precise;
}

bool TimeStamp::operator >= (TimeStamp other) const noexcept {
    return precise >= other.precise;
}

const TimeStamp TimeStamp::MAX_TIMESTAMP = {std::numeric_limits<int64_t>::max()};

const TimeStamp TimeStamp::MIN_TIMESTAMP = {0L};

//------------------------

Entry::Entry(uint32_t length)
    : length(length)
    , time {}
    , param_id {}
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


PageCursor::PageCursor(uint32_t* buffer, uint64_t buffer_size) noexcept
    : results(buffer)
    , results_cap(buffer_size)
    , results_num(0u)
    , done(0u)
    , start_index(0u)
    , probe_index(0u)
    , state(AKU_CURSOR_START)
{
}


SingleParameterCursor::SingleParameterCursor
    ( ParamId      pid
    , TimeStamp    low
    , TimeStamp    upp
    , uint32_t     scan_dir
    , uint32_t*    buffer
    , uint64_t     buffer_size )  noexcept

    : PageCursor(buffer, buffer_size)
    , param(pid)
    , lowerbound(low)
    , upperbound(upp)
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
    , length(length)
    , overwrites_count(0)
    , page_id(page_id)
    , bbox()
{
}

int PageHeader::get_entries_count() const noexcept {
    return (int)this->count;
}

int PageHeader::get_free_space() const noexcept {
    auto begin = reinterpret_cast<const char*>(page_index + count);
    const char* end = 0;
    end = cdata() + last_offset;
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

void PageHeader::clear() noexcept {
    count = 0;
    overwrites_count++;
    last_offset = length - 1;
    bbox = PageBoundingBox();
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
    update_bounding_box(entry.param_id, entry.time);
    return AKU_WRITE_STATUS_SUCCESS;
}

int PageHeader::add_entry(Entry2 const& entry) noexcept {
    auto space_required = entry.range.length + sizeof(Entry2) + sizeof(EntryOffset);
    if (space_required > get_free_space()) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    char* free_slot = 0;
    free_slot = data() + last_offset;
    // FIXME: reorder to improve memory performance
    // Write data
    free_slot -= entry.range.length;
    memcpy((void*)free_slot, entry.range.address, entry.range.length);
    // Write length
    free_slot -= sizeof(uint32_t);
    *(uint32_t*)free_slot = entry.range.length;
    // Write paramId and timestamp
    free_slot -= sizeof(Entry2);
    memcpy((void*)free_slot, (void*)&entry, sizeof(Entry2));
    last_offset = free_slot - cdata();
    page_index[count] = last_offset;
    count++;
    update_bounding_box(entry.param_id, entry.time);
    return AKU_WRITE_STATUS_SUCCESS;
}

const Entry* PageHeader::read_entry(int index) const noexcept {
    if (index >= 0 && index < count) {
        auto offset = page_index[index];
        auto ptr = cdata() + offset;
        auto entry_ptr = reinterpret_cast<const Entry*>(ptr);
        return entry_ptr;
    }
    return 0;
}

int PageHeader::get_entry_length(int entry_index) const noexcept {
    auto entry_ptr = read_entry(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry(int index, Entry* receiver) const noexcept {
    auto entry_ptr = read_entry(index);
    if (entry_ptr) {
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, entry_ptr->length);
        return entry_ptr->length;
    }
    return 0;
}

template<class RandomIt, class Cmp>
void ins_sort(RandomIt start, RandomIt end, Cmp cmp) {
    for (RandomIt k = start; k < end; ++k) {
        RandomIt l = k;
        RandomIt l_prev = l - 1;
        while (l_prev >= start && cmp(*l_prev, *l)) {
            std::iter_swap(l, l_prev);
            l--;
            l_prev--;
        }
    }
}

void PageHeader::sort() noexcept {
    auto begin = page_index;
    auto end = page_index + count;
    gfx::timsort(begin, end, [&](EntryOffset a, EntryOffset b) {
        auto ea = reinterpret_cast<const Entry*>(cdata() + a);
        auto eb = reinterpret_cast<const Entry*>(cdata() + b);
        auto ta = std::tuple<uint64_t, uint32_t>(ea->time.precise, ea->param_id);
        auto tb = std::tuple<uint64_t, uint32_t>(eb->time.precise, eb->param_id);
        return ta < tb;
    });
}


/** Return false if cursor is ill-formed.
  * Status and error code fields will be changed accordignly.
  */
static bool validate_cursor(SingleParameterCursor *cursor) noexcept {
    // Cursor validation
    if ((cursor->direction != AKU_CURSOR_DIR_BACKWARD && cursor->direction != AKU_CURSOR_DIR_FORWARD) ||
         cursor->upperbound < cursor->lowerbound)
    {
        cursor->state = AKU_CURSOR_ERROR;
        cursor->error_code = AKU_SEARCH_EBAD_ARG;
        return false;
    }
    return true;
}


void PageHeader::search(SingleParameterCursor *cursor) const noexcept
{
    /* Search algorithm outline:
     * - interpolated search for timestamp
     *   - if 5 or more iterations or
     *     search interval is small
     *     BREAK;
     * - binary search for timestamp
     * - scan
     */

    if (!validate_cursor(cursor))
        return;

    // Reuse buffer
    cursor->results_num = 0;

    bool is_backward = cursor->direction == AKU_CURSOR_DIR_BACKWARD;
    ParamId param = cursor->param;
    uint32_t max_index = count - 1u;
    uint32_t begin = 0u;
    uint32_t end = max_index;
    int64_t key = is_backward ? cursor->upperbound.precise
                              : cursor->lowerbound.precise;
    uint32_t probe_index = 0u;

    while(1) {
        switch(cursor->state) {
        case AKU_CURSOR_START:
            cursor->state = AKU_CURSOR_SEARCH;
        case AKU_CURSOR_SEARCH:
            if (key <= bbox.max_timestamp.precise && key >= bbox.min_timestamp.precise) {

                int64_t search_lower_bound = bbox.min_timestamp.precise;
                int64_t search_upper_bound = bbox.max_timestamp.precise;

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
                        auto probe = probe_entry->time.precise;

                        if (probe == key) {
                            cursor->probe_index = probe_index;
                            cursor->start_index = probe_index;
                            cursor->state = is_backward ? AKU_CURSOR_SCAN_BACKWARD
                                                        : AKU_CURSOR_SCAN_FORWARD;
                            break;
                        } else if (probe < key) {
                            begin = probe_index + 1u;
                            probe_offset = page_index[begin];
                            probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                            search_lower_bound = probe_entry->time.precise;
                        } else {
                            end   = probe_index - 1u;
                            probe_offset = page_index[end];
                            probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                            search_upper_bound = probe_entry->time.precise;
                        }
                    }
                    else {
                        break;
                        // Continue with binary search
                    }
                }
            } else {
                // shortcut for corner cases
                if (key > bbox.max_timestamp.precise) {
                    if (is_backward) {
                        cursor->probe_index = end;
                        cursor->start_index = end;
                        cursor->state = AKU_CURSOR_SCAN_BACKWARD;
                        break;
                    } else {
                        // return empty result
                        cursor->state = AKU_CURSOR_COMPLETE;
                        return;
                    }
                }
                else if (key < bbox.min_timestamp.precise) {
                    if (!is_backward) {
                        cursor->probe_index = begin;
                        cursor->start_index = begin;
                        cursor->state = AKU_CURSOR_SCAN_FORWARD;
                        break;
                    } else {
                        // return empty result
                        cursor->state = AKU_CURSOR_COMPLETE;
                        return;
                    }
                }
            }
            while (end >= begin) {
                probe_index = begin + ((end - begin) / 2u);
                auto probe_offset = page_index[probe_index];
                auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                auto probe = probe_entry->time.precise;

                if (probe == key) {             // found
                    break;
                }
                else if (probe < key) {
                    begin = probe_index + 1u;   // change min index to search upper subarray
                    if (begin == count)         // we hit the upper bound of the array
                        break;
                } else {
                    end = probe_index - 1u;     // change max index to search lower subarray
                    if (end == ~0)              // we hit the lower bound of the array
                        break;
                }
            }
            cursor->probe_index = probe_index;
            cursor->start_index = probe_index;
            cursor->state = is_backward ? AKU_CURSOR_SCAN_BACKWARD
                                        : AKU_CURSOR_SCAN_FORWARD;
            break;
        case AKU_CURSOR_SCAN_BACKWARD:
            while (true) {
                auto current_index = cursor->probe_index--;
                auto probe_offset = page_index[current_index];
                auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                auto probe = probe_entry->param_id;
                bool probe_in_time_range = cursor->lowerbound <= probe_entry->time &&
                                           cursor->upperbound >= probe_entry->time;
                if (probe == param && probe_in_time_range) {
                    if (cursor->results_num < cursor->results_cap) {
                        cursor->results[cursor->results_num] = current_index;
                        cursor->results_num += 1u;
                    }
                    if (cursor->results_num == cursor->results_cap) {
                        return;
                    }
                }
                if (probe_entry->time < cursor->lowerbound || current_index == 0u) {
                    cursor->state = AKU_CURSOR_COMPLETE;
                    cursor->done = 1u;
                    return;
                }
            }
        case AKU_CURSOR_SCAN_FORWARD:
            while (true) {
                auto current_index = cursor->probe_index++;
                auto probe_offset = page_index[current_index];
                auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
                auto probe = probe_entry->param_id;
                bool probe_in_time_range = cursor->lowerbound <= probe_entry->time &&
                                           cursor->upperbound >= probe_entry->time;
                if (probe == param && probe_in_time_range) {
                    if (cursor->results_num < cursor->results_cap) {
                        cursor->results[cursor->results_num] = current_index;
                        cursor->results_num += 1u;
                    }
                    if (cursor->results_num == cursor->results_cap) {
                        return;
                    }
                }
                if (probe_entry->time > cursor->upperbound || current_index == max_index) {
                    cursor->state = AKU_CURSOR_COMPLETE;
                    cursor->done = 1u;
                    return;
                }
            }
        }
    }
}

}  // namepsace
