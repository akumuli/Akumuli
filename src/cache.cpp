/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */

#include <thread>
#include "akumuli_def.h"
#include "cache.h"
#include "util.h"

namespace Akumuli {


// This method must be called from the same thread
int Sequence::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    auto key = std::make_tuple(ts, param);

    std::unique_lock<std::mutex> lock(obj_mtx_, std::defer_lock)
                               , tmp_lock(tmp_mtx_, std::defer_lock);

    if (lock.try_lock()) {
        if (tmp_lock.try_lock()) {
            for(auto const& tup: temp_) {
                auto tkey = std::make_tuple(std::get<0>(tup), std::get<1>(tup));
                data_.insert(std::make_pair(tkey, std::get<2>(tup)));
            }
            tmp_lock.unlock();
        }
        data_.insert(std::make_pair(key, offset));
    } else {
        tmp_lock.lock();
        temp_.emplace_back(ts, param, offset);
    }
    return AKU_WRITE_STATUS_SUCCESS;
}

void Sequence::search(Caller& caller, InternalCursor* cursor, SingleParameterSearchQuery const& query) const noexcept {

    bool forward = query.direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = query.direction == AKU_CURSOR_DIR_BACKWARD;

    if (query.upperbound < query.lowerbound  // Right timestamps and
        || !(forward ^ backward)             // right direction constant
    ) {
        cursor->set_error(caller, AKU_EBAD_ARG);
        return;
    }

    if (backward)
    {
        auto tskey = query.upperbound;
        auto idkey = (ParamId)~0;
        auto key = std::make_tuple(tskey, idkey);
        auto citer = data_.upper_bound(key);
        auto last_key = std::make_tuple(query.lowerbound, 0);

        std::unique_lock<std::mutex> lock(obj_mtx_);
        while(true) {
            auto& curr_key = citer->first;
            if (std::get<0>(curr_key) <= std::get<0>(last_key)) {
                break;
            }
            if (std::get<1>(curr_key) == query.param && std::get<0>(curr_key) <= tskey) {
                cursor->put(caller, citer->second);
            }
            if (citer == data_.begin()) {
                break;
            }
            citer--;
        }
    }
    else
    {
        auto tskey = query.lowerbound;
        auto idkey = (ParamId)0;
        auto key = std::make_tuple(tskey, idkey);
        auto citer = data_.lower_bound(key);
        auto last_key = std::make_tuple(query.upperbound, ~0);

        std::unique_lock<std::mutex> lock(obj_mtx_);
        while(citer != data_.end()) {
            auto& curr_key = citer->first;
            if (std::get<0>(curr_key) >= std::get<0>(last_key)) {
                break;
            }
            if (std::get<1>(curr_key) == query.param) {
                cursor->put(caller, citer->second);
            }
            citer++;
        }
    }
    cursor->complete(caller);
}

size_t Sequence::size() const noexcept {
    return data_.size();
}

Sequence::MapType::const_iterator Sequence::begin() const {
    return data_.begin();
}

Sequence::MapType::const_iterator Sequence::end() const {
    return data_.end();
}


// Bucket -------------------------------------

Bucket::Bucket(int64_t size_limit, int64_t baseline)
    : baseline(baseline)
    , limit_(size_limit)
    , state(0)
{
}

int Bucket::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    if (limit_.dec()) {
        return seq_.local().add(ts, param, offset);
    }
    return AKU_EOVERFLOW;
}

void Bucket::search(Caller &caller, InternalCursor* cursor, SingleParameterSearchQuery const& query) const noexcept {
    for(SeqList::iterator i = seq_.begin(); i != seq_.end(); i++) {
        i->search(caller, cursor, query);
    }
}

typedef Sequence::MapType::const_iterator iter_t;

static bool less_than(iter_t lhs, iter_t rhs, PageHeader* page) {
    auto lentry = page->read_entry(lhs->second),
         rentry = page->read_entry(rhs->second);

    auto lkey = std::make_tuple(lentry->time, lentry->param_id),
         rkey = std::make_tuple(rentry->time, rentry->param_id);

    return lkey < rkey;
}

int Bucket::merge(Caller& caller, InternalCursor *cur, PageHeader* page) const noexcept {

    if (state.load() == 0) {
        return AKU_EBUSY;
    }

    size_t n = seq_.size();

    // Init
    iter_t iter[n], end[n];
    size_t cnt = 0u;
    for(SeqList::iterator i = seq_.begin(); i != seq_.end(); i++) {
        iter[cnt] = i->begin();
        end[cnt] = i->end();
        cnt++;
    }

    // Merge
    if (n > 1) {
        int next_min = 0;
        while(true) {
            int min = next_min;
            int op_cnt = 0;
            for (int i = 0; i < n; i++) {
                if (i == min) continue;
                if (iter[i] != end[i]) {
                    if (less_than(iter[i], iter[min], page)) {
                        next_min = min;
                        min = i;
                    } else {
                        next_min = i;
                    }
                    op_cnt++;
                }
            }
            if (op_cnt == 0)
                break;
            auto offset = iter[min]->second;
            cur->put(caller, offset);
            std::advance(iter[min], 1);
        }
        assert(iter == end);
    } else {
        for (const auto& pair: boost::make_iterator_range(iter[0], end[0])) {
            cur->put(caller, pair.second);
        }
    }
    return AKU_SUCCESS;
}

// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, size_t max_size)
    : ttl_(ttl)
    , max_size_(max_size)
    , baseline_()
{
    // We need to calculate shift width. So, we got ttl in some units
    // of measure (units of measure that akumuli doesn't know about).
    shift_ = log2(ttl.value);
    if ((1 << shift_) < AKU_LIMITS_MIN_TTL) {
        throw std::runtime_error("TTL is too small");
    }
}

template<class TCont>
Sequence* index2ptr(TCont& cont, int64_t index) noexcept {
    auto begin = cont.begin();
    std::advance(begin, index);
    auto& gen = *begin;
    return &gen;
}

template<class TCont>
Sequence const* index2ptr(TCont const& cont, int64_t index) noexcept {
    auto begin = cont.cbegin();
    std::advance(begin, index);
    auto& gen = *begin;
    return &gen;
}

int Cache::add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept {
    // NOTE: If it is less than zero - we need to shift cache.
    // Otherwise we can select existing generation but this can result in late write
    // or overflow.
    auto absolute_index = (ts.value >> shift_);

    TableType::accessor accessor;
    if (cache_.find(accessor, absolute_index)) {
        if (accessor->second->state == 0) {
            return accessor->second->add(ts, pid, offset);
        }
    }
    else {
        std::lock_guard<LockType> guard(lock_);
        auto rel_index = baseline_ - absolute_index;
        if (!cache_.find(accessor, absolute_index)) {
            if (rel_index > AKU_LIMITS_MAX_CACHES) {
                return AKU_ELATE_WRITE;
            }
            if (rel_index < 0) {
                // Future write! Mark all outdated buckets.
                auto size = cache_.size();
                auto min_baseline = absolute_index - AKU_LIMITS_MAX_CACHES;
                for(auto& b: live_buckets_) {
                    if (b->baseline < min_baseline) {
                        auto old = b->state++;
                        nswapped += old;
                    }
                }
            }
            // bucket is not already created by another thread
            size_t bucket_size = sizeof(Bucket);
            Bucket* new_bucket = allocator_.allocate(bucket_size);
            allocator_.construct(new_bucket, (int64_t)max_size_, baseline_);
            cache_.insert(std::make_pair(absolute_index, new_bucket));
            live_buckets_.push_front(new_bucket);
            return new_bucket->add(ts, pid, offset);
        }
        else {
            if (accessor->second->state == 0) {
                return accessor->second->add(ts, pid, offset);
            }
        }
    }
    return AKU_ELATE_WRITE;
}

int Cache::add_entry(const Entry& entry, EntryOffset offset, size_t* nswapped) noexcept {
    return add_entry_(entry.time, entry.param_id, offset, nswapped);
}

int Cache::add_entry(const Entry2& entry, EntryOffset offset, size_t* nswapped) noexcept {
    return add_entry_(entry.time, entry.param_id, offset, nswapped);
}

void Cache::clear() noexcept {
}

int Cache::remove_old(EntryOffset* offsets, size_t size, uint32_t* noffsets) noexcept {
    return AKU_EGENERAL;
}

void Cache::search(Caller& caller, InternalCursor *cur, SingleParameterSearchQuery& query) const noexcept {

    bool forward = query.direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = query.direction == AKU_CURSOR_DIR_BACKWARD;

    if (query.upperbound < query.lowerbound
        || !(forward ^ backward)
    ) {
        // Invalid direction or timestamps
        cur->set_error(caller, AKU_EBAD_ARG);
        return;
    }

//    if (cursor->cache_init == 0) {
//        // Init search
//        auto tskey = cursor->upperbound.value;
//        auto idkey = cursor->param;
//        auto key = tskey >> shift_;
//        auto index = baseline_ - key;
//        // PROBLEM: index can change between calls
//        if (index < 0) {
//            // future read
//            index = 0;
//        }
//        cursor->cache_init = 1;
//        cursor->cache_index = 0;
//        cursor->cache_start_id = key;
//    }
    throw std::runtime_error("Not implemented");
}

}  // namespace Akumuli
