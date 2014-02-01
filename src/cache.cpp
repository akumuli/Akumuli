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

Generation::Generation(TimeDuration ttl, size_t max_size) noexcept
    : ttl_(ttl)
    , capacity_(max_size)
    , most_recent_(TimeStamp::MIN_TIMESTAMP)
{
}

Generation::Generation(Generation const& other)
    : ttl_(other.ttl_)
    , capacity_(other.capacity_)
    , data_(other.data_)
    , most_recent_(other.most_recent_)
{
}

void Generation::swap(Generation& other) {
    // NOTE: most_recent_ doesn't swapped intentionaly
    // I want to propagate it while moving generations back
    auto tmp_ttl = ttl_;
    auto tmp_cap = capacity_;
    data_.swap(other.data_);
    ttl_ = other.ttl_;
    capacity_ = other.capacity_;
    other.ttl_ = tmp_ttl;
    other.capacity_ = tmp_cap;
}

bool Generation::get_oldest_timestamp(TimeStamp* ts) const noexcept {
    if (data_.empty())
        return false;
    auto iter = data_.begin();
    *ts = std::get<0>(iter->first);
    return true;
}

bool Generation::get_most_recent_timestamp(TimeStamp* ts) const noexcept {
    if (most_recent_ == TimeStamp::MIN_TIMESTAMP)
        return false;
    *ts = most_recent_;
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
    for (;iter_pair.first != iter_pair.second; ++iter_pair.first) {
        if (skip) {
            skip--;
        } else {
            if (result_ix == results_len) break;
            results[result_ix++] = iter_pair.first->second;
        }
    }
    return std::make_pair(result_ix, iter_pair.first != iter_pair.second);
}

void Generation::search(SingleParameterCursor* cursor) const noexcept {
    cursor->results_num = 0;                        // NOTE: search always performed, for better performance
    auto tskey = cursor->upperbound;                //       caller must use large enough buffer, to be able
    auto idkey = (ParamId)~0;                       //       to process all the data with a single call!
    auto key = std::make_tuple(tskey, idkey);       //       cursor->state is ignored, only output indexes is used
    auto citer = data_.upper_bound(key);
    auto skip = cursor->skip;

    /// SKIP ///
    for (uint32_t i = 0; i < cursor->skip; i++) {
        if (citer == data_.begin()) {
            cursor->state = AKU_CURSOR_COMPLETE;
            return;
        }
        citer--;
    }

    /// SCAN ///
    auto last_key = std::make_tuple(cursor->lowerbound, 0);

    while(true) {
        skip++;
        auto& curr_key = citer->first;
        if (std::get<0>(curr_key) <= std::get<0>(last_key)) {
            cursor->state = AKU_CURSOR_COMPLETE;
            return;
        }
        if (std::get<1>(curr_key) == cursor->param && std::get<0>(curr_key) <= tskey) {
            cursor->results[cursor->results_num] = citer->second;
            cursor->results_num++;
            if (cursor->results_num == cursor->results_cap) {
                // yield data to caller
                cursor->state = AKU_CURSOR_SCAN_BACKWARD;
                cursor->skip = skip;
                return;
            }
        }
        if (citer == data_.begin()) {
            cursor->state = AKU_CURSOR_COMPLETE;
            return;
        }
        citer--;
    }
}

size_t Generation::size() const noexcept {
    return data_.size();
}

Generation::MapType::const_iterator Generation::begin() const {
    return data_.begin();
}

Generation::MapType::const_iterator Generation::end() const {
    return data_.end();
}

void Generation::close() {
    auto ptr = data_.begin();
    auto ix = data_.size() - 1;
    if (ix < 0) {
        throw std::logic_error("Can't close empty generation.");
    }
    std::advance(ptr, ix);
    most_recent_ = std::get<0>(ptr->first);
}

// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, size_t max_size)
    : ttl_(ttl)
    , max_size_(max_size)
{
    // First created generation will hold elements with indexes
    // from offset_ to offset_ + gen.size()
    gen_.push_back(Generation(ttl_, max_size_));
}

int Cache::add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset) noexcept {
    auto& gen0 = gen_[0];
    int status = gen0.add(ts, pid, offset);
    switch(status) {
    case AKU_WRITE_STATUS_OVERFLOW: {
            // Rotate
            gen0.close();
            gen_.emplace_back(ttl_, max_size_);
            auto curr = gen_.rbegin();
            auto prev = gen_.rbegin();
            std::advance(curr, 1);
            while(curr != gen_.rend()) {
                curr->swap(*prev);
                prev = curr;
                std::advance(curr, 1);
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

void Cache::clear() noexcept {
    gen_.clear();
    gen_.push_back(Generation(ttl_, max_size_));
}

int Cache::remove_old(EntryOffset* offsets, size_t size, uint32_t* noffsets) noexcept {
    auto ngen = gen_.size();
    if (ngen > 2) {
        return AKU_ENO_DATA;
    }
    Generation& last = gen_.back();
    auto lastsize = last.size();
    if (lastsize > size) {
        return AKU_ENO_MEM;
    }
    *noffsets = lastsize;

    int ix = 0;
    for(auto it = last.begin(); it != last.end(); ++it) {
        offsets[ix++] = it->second;
    }
    return AKU_EGENERAL;
}

bool Cache::is_too_late(TimeStamp ts) noexcept {
    Generation& gen = gen_[0];
    TimeStamp div;
    bool has_most_recent = gen.get_most_recent_timestamp(&div);
    if (has_most_recent) {
        // most frequent path
        return ts < div;
    }
    // least frequent path
    return false;
}

void Cache::search(SingleParameterCursor* cursor) const noexcept {
    throw std::runtime_error("Not implemented");
    while(true) {
        switch(cursor->state) {
        case AKU_CURSOR_START:
            // Init search
            cursor->generation = 0;
            cursor->state = AKU_CURSOR_SEARCH;
        case AKU_CURSOR_SEARCH: {
                // Search in single generation
                Generation const& gen = gen_[cursor->generation];
                gen.search(cursor);
            }
            break;
        };
    }
}

}  // namespace Akumuli
