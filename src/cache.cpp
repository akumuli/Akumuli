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

void Generation::swap(Generation& other) {
    auto tmp_ttl = ttl_;
    auto tmp_cap = capacity_;
    auto tmp_six = starting_index_;
    data_.swap(other.data_);
    ttl_ = other.ttl_;
    capacity_ = other.capacity_;
    starting_index_ = other.starting_index_;
    other.ttl_ = tmp_ttl;
    other.capacity_ = tmp_cap;
    other.starting_index_ = tmp_six;
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

size_t Generation::offset() const noexcept {
    return this->starting_index_;
}

Generation::MapType::const_iterator Generation::begin() const {
    return data_.begin();
}

Generation::MapType::const_iterator Generation::end() const {
    return data_.end();
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

int Cache::add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset) noexcept {
    int status = gen_[0].add(ts, pid, offset);
    switch(status) {
    case AKU_WRITE_STATUS_OVERFLOW: {
            // Rotate
            gen_.emplace_back(ttl_, max_size_, gen_[0].offset() + gen_[0].size());
            auto curr = gen_.rbegin();
            auto prev = gen_.rbegin();
            std::advance(curr, 1);
            while(curr != gen_.rend()) {
                curr->swap(*prev);
            }
        }
    case AKU_WRITE_STATUS_SUCCESS:
        break;
    };
    return status;
}

int Cache::add_entry(const Entry& entry, EntryOffset offset) noexcept {
    return add_entry_(entry.time, entry.param_id, offset);
}

int Cache::add_entry(const Entry2& entry, EntryOffset offset) noexcept {
    return add_entry_(entry.time, entry.param_id, offset);
}

void Cache::close() noexcept {
}

int Cache::remove_old(EntryOffset* offsets, size_t size, uint32_t* start_index, uint32_t* noffsets) noexcept {
    auto ngen = gen_.size();
    if (ngen > 2) {
        return AKU_ENO_DATA;
    }
    Generation& last = gen_.back();
    auto lastsize = last.size();
    if (lastsize > size) {
        return AKU_ENO_MEM;
    }
    *start_index = last.offset();
    *noffsets = lastsize;

    int ix = 0;
    for(auto it = last.begin(); it != last.end(); ++it) {
        offsets[ix++] = it->second;
    }

    return AKU_EGENERAL;

}

}  // namespace Akumuli
