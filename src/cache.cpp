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
    , baseline_{}
{
    // First created generation will hold elements with indexes
    // from offset_ to offset_ + gen.size()
    for(int i = 0; i < AKU_LIMITS_MAX_CACHES; i++) {
        gen_.push_back(Generation(max_size_));
    }

    // We need two calculate shift width. So, we got ttl in some units
    // of measure, units of measure that akumuli doesn't know about.
    // We choose first power of two that is less than ttl:
    shift_ = log2(ttl.value);
    if ((1 << shift_) < AKU_LIMITS_MIN_TTL) {
        throw std::runtime_error("TTL is too small");
    }
}

void Cache::swapn(int swaps) noexcept {
    for(auto it = gen_.rbegin(); it != gen_.rend(); ++it) {
        swap_.emplace_back(max_size_);
        it->swap(swap_.back());
        if (--swaps == 0) break;
    }
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
            gen = &gen_[index];
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
            gen_.clear();
            for (int i = 0; i < AKU_LIMITS_MAX_CACHES; i++) {
                gen_.emplace_back(max_size_);
            }
        }
        else {
            // Calculate, how many gen-s must be swapped
            auto freeslots = AKU_LIMITS_MAX_CACHES - count;
            if (freeslots < gen_.size()) {
                size_t swapscnt = gen_.size() - freeslots;
                swapn(swapscnt);
                *nswapped += swapscnt;
            }
            for (int i = 0; i < count; i++) {
                // This is quadratic algo. but
                // gen_.size() is limited by the
                // small number.
                gen_.emplace_back(max_size_);
                auto curr = gen_.rbegin();
                auto prev = gen_.rbegin();
                std::advance(curr, 1);
                while(curr != gen_.rend()) {
                    curr->swap(*prev);
                    prev = curr;
                    std::advance(curr, 1);
                }
            }
        }
        gen = &gen_[index];
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
    gen_.clear();
    for (int i = 0; i < AKU_LIMITS_MAX_CACHES; i++)
        gen_.emplace_back(max_size_);
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
