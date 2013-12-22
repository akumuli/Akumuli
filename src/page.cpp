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


PageCursor::PageCursor(int* buffer, size_t buffer_size) noexcept
    : results(buffer)
    , results_cap(buffer_size)
    , results_num(0)
    , done(false)
    , start_index(0)
    , probe_index(0)
    , state(AKU_CURSOR_START)
{
}


SingleParameterCursor::SingleParameterCursor
    ( ParamId      pid
    , TimeStamp    low
    , TimeStamp    upp
    , int*         buffer
    , size_t       buffer_size )  noexcept

    : PageCursor(buffer, buffer_size)
    , param(pid)
    , lowerbound(low)
    , upperbound(upp)
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
    , last_offset(length)
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
    last_offset = length;
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

bool PageHeader::search
    ( ParamId param
    , TimeStamp time_lowerbound
    , EntryOffset* out_offset
    ) const noexcept
{
    // NOTE: this is binary search implementation
    // it perform binary search using timestamp and that scans
    // back to the begining of the page to find correct param_id.
    // It supposed to be replaced with interpolation search in future versions.
    uint32_t begin = 0u;
    uint32_t end = count;
    auto key = time_lowerbound.precise;
    uint32_t found_index = 0;
    bool is_found = false;
    while (end >= begin) {
        auto probe_index = begin + ((end - begin) / 2);
        auto probe_offset = page_index[probe_index];
        auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
        auto probe = probe_entry->time.precise;
        if (probe == key) {
            // found
            begin = probe_index;
            break;
        }
        // determine which subarray to search
        else if (probe < key) {
            // change min index to search upper subarray
            begin = probe_index + 1;
        } else {
            // change max index to search lower subarray
            end = probe_index - 1;
        }
    }

    // Trace back
    auto probe_index = begin;
    while (true) {
        auto probe_offset = page_index[probe_index];
        auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
        auto probe = probe_entry->param_id;
        if (probe == param) {
            is_found = true;
            found_index = probe_index;
            break;
        }
        if (probe_index == 0)
            break;
        probe_index--;
    }

    if (is_found)
        *out_offset = found_index;

    return is_found;
}

void PageHeader::search(SingleParameterCursor *cursor) const noexcept
{
    ParamId param = cursor->param;
    uint32_t begin = 0u;
    uint32_t end = count - 1;
    auto key = cursor->upperbound.precise;
    uint32_t probe_index = 0;
    switch(cursor->state) {
    case AKU_CURSOR_START:
        cursor->state = AKU_CURSOR_SEARCH;
    case AKU_CURSOR_SEARCH:
        if (key <= bbox.max_timestamp.precise && key >= bbox.min_timestamp.precise) {
            // Perform interpolation search first
            auto step = double(bbox.max_timestamp.precise - bbox.min_timestamp.precise) / count;
            probe_index = static_cast<uint32_t>((key - bbox.min_timestamp.precise) / step);
            assert(probe_index >= begin);
            assert(probe_index <= end);
            auto probe_offset = page_index[probe_index];
            auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
            auto probe = probe_entry->time.precise;
            if (probe == key) {
                begin = probe_index;
                end = probe_index - 1;
            } else if (probe < key) {
                begin = probe_index + 1;
            } else {
                end = probe_index - 1;
            }
        }
        while (end >= begin) {
            probe_index = begin + ((end - begin) / 2);
            auto probe_offset = page_index[probe_index];
            auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
            auto probe = probe_entry->time.precise;
            if (probe == key) {
                // found
                break;
            }
            // determine which subarray to search
            else if (probe < key) {
                // change min index to search upper subarray
                begin = probe_index + 1;
            } else {
                // change max index to search lower subarray
                end = probe_index - 1;
            }
        }
        cursor->probe_index = probe_index;
        cursor->start_index = probe_index;
        cursor->state = AKU_CURSOR_SCAN_BACKWARD;
    case AKU_CURSOR_SCAN_BACKWARD:
        // Trace back
        while (true) {
            auto probe_offset = page_index[cursor->probe_index];
            auto probe_entry = reinterpret_cast<const Entry*>(cdata() + probe_offset);
            auto probe = probe_entry->param_id;
            if (probe == param) {
                if (cursor->results_num < cursor->results_cap) {
                    cursor->results[cursor->results_num] = cursor->probe_index;
                    cursor->results_num += 1;
                }
                if (cursor->results_num == cursor->results_cap) {
                    return;
                }
            }
            if (cursor->lowerbound >= probe_entry->time ||
                cursor->probe_index == 0) {
                cursor->state = AKU_CURSOR_COMPLETE;
                cursor->done = true;
                return;
            }
            cursor->probe_index--;
        }
    }
}

}  // namepsace
