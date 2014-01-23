/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */

#define AKU_NUM_GENERATIONS 5

#include "cache.h"

namespace Akumuli {

// Generation ---------------------------------

Generation::Generation(TimeDuration ttl, size_t max_size, uint32_t starting_index) noexcept
    : ttl_(ttl)
    , capacity_(max_size)
    , starting_index_(starting_index)
{
}

Generation::Generation(Generation const& other)
    : ttl_(other.ttl_)
    , capacity_(other.capacity_)
    , starting_index_(other.starting_index_)
    , data_(other.data_)
{
}

Generation::Generation(Generation && other) noexcept
    : ttl_(other.ttl_)
    , capacity_(other.capacity_)
    , starting_index_(other.starting_index_)
{
    other.data_.swap(data_);
}

bool Generation::get_oldest_timestamp(TimeStamp* ts) const noexcept {
    if (data_.empty())
        return false;
    auto iter = data_.begin();
    *ts = std::get<0>(iter->first);
    return true;
}

int Generation::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    auto key = std::make_tuple(ts, param);
    data_.insert(std::make_pair(key, offset));
    TimeStamp oldest;
    get_oldest_timestamp(&oldest);
    TimeDuration diff = ts - oldest;
    if (diff.value > ttl_.value || capacity_ == 0) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    capacity_--;
    return AKU_WRITE_STATUS_SUCCESS;
}

std::pair<size_t, bool> Generation::find(TimeStamp ts, ParamId pid, EntryOffset* results, size_t results_len, size_t skip) noexcept {
    auto key = std::make_tuple(ts, pid);
    auto iter_pair = data_.equal_range(key);
    size_t result_ix = 0;
    for (;iter_pair.first != iter_pair.second; iter_pair.first++) {
        if (skip) {
            skip--;
        } else {
            if (result_ix == results_len) break;
            results[result_ix++] = iter_pair.first->second;
        }
    }
    return std::make_pair(result_ix, iter_pair.first != iter_pair.second);
}

size_t Generation::size() const noexcept {
    return data_.size();
}



// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, PageHeader* page, size_t max_size)
    : ttl_(ttl)
    , page_(page)
    , max_size_(max_size)
{
    // Cache starting offset
    offset_ = page_->count;

    // First created generation will hold elements with indexes
    // from offset_ to offset_ + gen.size()
    gen_.push_back(Generation(ttl_, max_size_, offset_));
}

void Cache::add_entry(const Entry& entry, EntryOffset offset) noexcept {
    //gen_[0].add(entry.time, entry.param_id, offset);
}

void Cache::add_entry(const Entry2& entry, EntryOffset offset) noexcept {
}

void Cache::close() noexcept {
}

}  // namespace Akumuli
