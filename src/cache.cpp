/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */

#include "akumuli_def.h"
#include "cache.h"
#include "util.h"

namespace Akumuli {

// Generation ---------------------------------

Generation::Generation(size_t max_size) noexcept
    : capacity_(max_size)
{
}

Generation::Generation(Generation const& other)
    : capacity_(other.capacity_)
    , data_(other.data_)
{
}

void Generation::swap(Generation& other) {
    auto tmp_cap = capacity_;
    data_.swap(other.data_);
    capacity_ = other.capacity_;
    other.capacity_ = tmp_cap;
}

int Generation::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    if (capacity_ == 0) {
        return AKU_WRITE_STATUS_OVERFLOW;
    }
    auto key = std::make_tuple(ts, param);
    data_.insert(std::make_pair(key, offset));
    capacity_--;
    return AKU_WRITE_STATUS_SUCCESS;
}

void Generation::search(SingleParameterCursor* cursor) const noexcept {

    // NOTE: search always performed, for better performance
    //       caller must use large enough buffer, to be able
    //       to process all the data with a single call!
    //       cursor->state is ignored, only output indexes is used

    bool forward = cursor->direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = cursor->direction == AKU_CURSOR_DIR_BACKWARD;

    if (cursor->upperbound < cursor->lowerbound
        || !(forward ^ backward)
        || cursor->results == nullptr
        || cursor->results_cap == 0
    ) {
        // Invalid direction or timestamps
        cursor->state = AKU_CURSOR_COMPLETE;
        cursor->error_code = AKU_EBAD_ARG;
        return;
    }

    cursor->results_num = 0;

    if (backward)
    {
        auto tskey = cursor->upperbound;
        auto idkey = (ParamId)~0;
        auto key = std::make_tuple(tskey, idkey);
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
    else
    {
        auto tskey = cursor->lowerbound;
        auto idkey = (ParamId)0;
        auto key = std::make_tuple(tskey, idkey);
        auto citer = data_.lower_bound(key);
        auto skip = cursor->skip;

        /// SKIP ///
        for (uint32_t i = 0; i < cursor->skip; i++) {
            if (citer == data_.end()) {
                cursor->state = AKU_CURSOR_COMPLETE;
                return;
            }
            citer++;
        }

        /// SCAN ///
        auto last_key = std::make_tuple(cursor->upperbound, ~0);

        while(citer != data_.end()) {
            auto& curr_key = citer->first;
            if (std::get<0>(curr_key) >= std::get<0>(last_key)) {
                cursor->state = AKU_CURSOR_COMPLETE;
                return;
            }
            skip++;
            if (std::get<1>(curr_key) == cursor->param) {
                cursor->results[cursor->results_num] = citer->second;
                cursor->results_num++;
                if (cursor->results_num == cursor->results_cap) {
                    // yield data to caller
                    cursor->state = AKU_CURSOR_SCAN_FORWARD;
                    cursor->skip = skip;
                    return;
                }
            }
            citer++;
        }
        cursor->state = AKU_CURSOR_COMPLETE;
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


// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, size_t max_size)
    : ttl_(ttl)
    , max_size_(max_size)
    , baseline_()
{
    // Cache prepopulation
    for (int i = 0; i < AKU_CACHE_POPULATION; i++) {
        cache_.emplace_back(max_size_);
        Generation& g = cache_.back();
        free_list_.push_back(g);
    }

    allocate_from_free_list(AKU_LIMITS_MAX_CACHES);

    // We need two calculate shift width. So, we got ttl in some units
    // of measure, units of measure that akumuli doesn't know about.
    // We choose first power of two that is less than ttl:
    shift_ = log2(ttl.value);
    if ((1 << shift_) < AKU_LIMITS_MIN_TTL) {
        throw std::runtime_error("TTL is too small");
    }
}

template<class TCont>
Generation* index2ptr(TCont& cont, int64_t index) noexcept {
    auto begin = cont.begin();
    std::advance(begin, index);
    auto& gen = *begin;
    return &gen;
}

template<class TCont>
Generation const* index2ptr(TCont const& cont, int64_t index) noexcept {
    auto begin = cont.cbegin();
    std::advance(begin, index);
    auto& gen = *begin;
    return &gen;
}

// [gen_] -(swaps)-> [swap_]
void Cache::swapn(int swaps) noexcept {
    auto end = gen_.end();
    auto begin = gen_.end();
    std::advance(begin, -1*swaps);
    gen_.splice(swap_.begin(), swap_, begin, end);
}

// [free_list_] -(ngens)-> [gen_]
void Cache::allocate_from_free_list(int ngens) noexcept {
    // TODO: add backpressure to producer to limit memory usage!
    auto n = free_list_.size();
    if (n < ngens) {
        // create new generations
        auto nnew = ngens - n;
        for (int i = 0; i < nnew; i++) {
            cache_.emplace_back(max_size_);
            auto& g = cache_.back();
            free_list_.push_back(g);
        }
    }
    auto begin = free_list_.begin();
    auto end = free_list_.begin();
    std::advance(end, ngens);
    free_list_.splice(gen_.begin(), gen_, begin, end);
}

int Cache::add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept {
    // TODO: set upper limit to ttl_ value to prevent overflow
    auto rts = (ts.value >> shift_);
    auto index = baseline_.value - rts;

    // NOTE: If it is less than zero - we need to shift cache.
    // Otherwise we can select existing generation but this can result in late write
    // or overflow.

    Generation* gen = nullptr;
    if (index >= 0) {
        if (index < gen_.size()) {
            if (index == 0) {
                // shortcut for must frequent case
                gen = &gen_.front();
            }
            else {
                gen = index2ptr(gen_, index);
            }
        }
        else {
            // Late write detected
            return AKU_WRITE_STATUS_OVERFLOW;
        }
    }
    else {
        // Future write! This must be ammortized across many writes.
        // If this procedure performs often - than we choose too small TTL
        auto count = 0 - index;
        baseline_.value = rts;
        index = 0;

        if (count >= AKU_LIMITS_MAX_CACHES) {
            // Move all items to swap
            size_t nswaps = gen_.size();
            swapn(nswaps);
            *nswapped += nswaps;
            allocate_from_free_list(AKU_LIMITS_MAX_CACHES);
        }
        else {
            // Calculate, how many gen-s must be swapped
            auto freeslots = AKU_LIMITS_MAX_CACHES - count;
            if (freeslots < gen_.size()) {
                size_t swapscnt = gen_.size() - freeslots;
                swapn(swapscnt);
                *nswapped += swapscnt;
            }
            allocate_from_free_list(count);
        }
        gen = index2ptr(gen_, index);
    }
    // add to bucket
    return gen->add(ts, pid, offset);
}

int Cache::add_entry(const Entry& entry, EntryOffset offset, size_t* nswapped) noexcept {
    return add_entry_(entry.time, entry.param_id, offset, nswapped);
}

int Cache::add_entry(const Entry2& entry, EntryOffset offset, size_t* nswapped) noexcept {
    return add_entry_(entry.time, entry.param_id, offset, nswapped);
}

void Cache::clear() noexcept {
    gen_.splice(free_list_.begin(), free_list_, gen_.begin(), gen_.end());
    swap_.splice(free_list_.begin(), free_list_, swap_.begin(), swap_.end());
}

int Cache::remove_old(EntryOffset* offsets, size_t size, uint32_t* noffsets) noexcept {
    return AKU_EGENERAL;
}

void Cache::search(SingleParameterCursor* cursor) const noexcept {

    bool forward = cursor->direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = cursor->direction == AKU_CURSOR_DIR_BACKWARD;

    if (cursor->upperbound < cursor->lowerbound
        || !(forward ^ backward)
        || cursor->results == nullptr
        || cursor->results_cap == 0
    ) {
        // Invalid direction or timestamps
        cursor->state = AKU_CURSOR_COMPLETE;
        cursor->error_code = AKU_EBAD_ARG;
        return;
    }

    if (cursor->cache_init == 0) {
        // Init search
        auto key = tskey >> shift_;
        auto index = baseline_.value - key;
        // PROBLEM: index can change between calls
        if (index < 0) {
            // future read
            index = 0;
        }
        cursor->cache_init = 1;
        cursor->cache_index = 0;
        cursor->cache_start_id = key;
    }
    throw std::runtime_error("Not implemented");

    auto tskey = cursor->upperbound.value;
    auto idkey = cursor->param;


    while(true) {
        switch(cursor->state) {
        case AKU_CURSOR_START: {
            cursor->generation = index;
            cursor->state = AKU_CURSOR_SEARCH;
            cursor->skip = 0;
        }
        case AKU_CURSOR_SEARCH: {
                // Search in single generation
                auto gen_index = cursor->generation;
                Generation const* gen = index2ptr(gen_, cursor->generation);
                gen->search(cursor);
                if (cursor->state == AKU_CURSOR_COMPLETE) {
                    cursor->generation++;
                    cursor->state = AKU_CURSOR_SEARCH;
                    return;
                }
            }
            break;
        };
    }
}

}  // namespace Akumuli
