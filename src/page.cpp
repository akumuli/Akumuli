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
#include "page.h"


namespace Spatium {
namespace Index {
Entry::Entry(uint32_t length)
    : length(length)
{
}


Entry::Entry(uint32_t param_id, TimeStamp timestamp, uint32_t length)
    : param_id(param_id)
    , time(time)
    , length(length)
{
}

const char* PageHeader::cdata() const noexcept {
    return reinterpret_cast<const char*>(this);
}

char* PageHeader::data() noexcept {
    return reinterpret_cast<char*>(this);
}

PageHeader::PageHeader(PageType type, uint16_t count, uint16_t length)
    : type(type)
    , count(count)
    , length(length)
{
}

int PageHeader::get_entries_count() const noexcept {
    return (int)this->count;
}

int PageHeader::get_free_space() const noexcept {
    auto begin = reinterpret_cast<const char*>(page_index + count);
    const char* end = 0;
    if (count) {
        end = cdata() + page_index[count - 1];
    }
    else {
        end = cdata() + length;
    }
    return end - begin;
}

PageHeader::AddStatus PageHeader::add_entry(Entry const& entry) noexcept {
    auto space_required = entry.length + sizeof(EntryOffset);
    if (entry.length < sizeof(Entry)) {
        return AddStatus::BadEntry;
    }
    if (space_required > get_free_space()) {
        return AddStatus::Overflow;
    }
    char* free_slot = 0;
    if (count) {
        free_slot = data() + page_index[count - 1];
    }
    else {
        free_slot = data() + length;
    }
    free_slot -= entry.length;
    memcpy((void*)free_slot, (void*)&entry, entry.length);
    page_index[count] = free_slot - cdata();
    count++;
    return AddStatus::Success;
}

const Entry* PageHeader::find_entry(int index) const noexcept {
    if (index >= 0 && index < count) {
        auto offset = page_index[index];
        auto ptr = cdata() + offset;
        auto entry_ptr = reinterpret_cast<const Entry*>(ptr);
        return entry_ptr;
    }
    return 0;
}

int PageHeader::get_entry_length(int entry_index) const noexcept {
    auto entry_ptr = find_entry(entry_index);
    if (entry_ptr) {
        return entry_ptr->length;
    }
    return 0;
}

int PageHeader::copy_entry(int index, Entry* receiver) const noexcept {
    auto entry_ptr = find_entry(index);
    if (entry_ptr) {
        if (entry_ptr->length > receiver->length) {
            return -1*entry_ptr->length;
        }
        memcpy((void*)receiver, (void*)entry_ptr, entry_ptr->length);
    }
    return 0;
}

}}  // namepsaces
