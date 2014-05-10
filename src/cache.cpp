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
            temp_.clear();
            tmp_lock.unlock();
        }
        data_.insert(std::make_pair(key, offset));
    } else {
        tmp_lock.lock();
        temp_.emplace_back(ts, param, offset);
    }
    return AKU_WRITE_STATUS_SUCCESS;
}

void Sequence::search(Caller& caller, InternalCursor* cursor, SearchQuery const& query) const noexcept {

    bool forward = query.direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = query.direction == AKU_CURSOR_DIR_BACKWARD;

    auto id_upper = (ParamId)~0;
    auto key_upper = std::make_tuple(query.upperbound, id_upper);

    auto id_lower = (ParamId)0;
    auto key_lower = std::make_tuple(query.lowerbound, id_lower);

    std::unique_lock<std::mutex> lock(obj_mtx_);

    auto it_upper = data_.upper_bound(key_upper);
    auto it_lower = data_.lower_bound(key_lower);

    auto match = [&query](MapType::const_iterator i) {
        auto& curr_key = i->first;
        return query.param_pred(std::get<1>(curr_key)) == SearchQuery::MATCH;
    };

    if (it_lower == data_.end()) {
        return;
    }

    if (it_upper == it_lower) {
        return;
    }

    if (backward)
    {
        it_upper--;

        for(; it_lower != it_upper; it_upper--) {
            if (match(it_upper)) {
                cursor->put(caller, it_upper->second);
            }
        }

        if (it_lower != data_.end()) {
            if (match(it_lower)) {
                cursor->put(caller, it_lower->second);
            }
        }
    }
    else
    {
        for(; it_lower != it_upper; it_lower++) {
            if (match(it_lower)) {
                cursor->put(caller, it_lower->second);
            }
        }
    }
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

void Bucket::search(Caller &caller, InternalCursor* cursor, SearchQuery const& query) const noexcept {
    // NOTE: Quick and dirty implementation that copies all the local data
    // to temporary sequence.
    std::unique_ptr<Sequence> seq(new Sequence());
    for(SeqList::iterator i = seq_.begin(); i != seq_.end(); i++) {
        std::unique_lock<std::mutex> lock(i->obj_mtx_);
        for (auto local = i->begin(); local != i->end(); local++) {
            auto ts = std::get<0>(local->first);
            auto id = std::get<1>(local->first);
            auto offset = local->second;
            seq->add(ts, id, offset);
        }
    }
    seq->search(caller, cursor, query);
}

typedef Sequence::MapType::const_iterator iter_t;

int Bucket::merge(Caller& caller, InternalCursor *cur) const noexcept {
    if (state.load() == 0) {
        return AKU_EBUSY;
    }

    size_t n = seq_.size();
    iter_t iter[n], ends[n];
    int cnt = 0;
    for(auto i = seq_.begin(); i != seq_.end(); i++) {
        iter[cnt] = i->begin();
        ends[cnt] = i->end();
        cnt++;
    }

    typedef std::tuple<TimeStamp, ParamId, EntryOffset, int> HeapItem;
    typedef std::vector<HeapItem> Heap;
    Heap heap;

    for(int index = 0; index < n; index++) {
        if (iter[index] != ends[index]) {
            auto value = *iter[index];
            iter[index]++;
            auto ts = std::get<0>(value.first);
            auto id = std::get<1>(value.first);
            auto offset = value.second;
            heap.push_back(std::make_tuple(ts, id, offset, index));
        }
    }

    std::make_heap(heap.begin(), heap.end(), std::greater<Heap::value_type>());

    while(!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), std::greater<Heap::value_type>());
        auto item = heap.back();
        auto offset = std::get<2>(item);
        int index = std::get<3>(item);
        cur->put(caller, offset);
        heap.pop_back();
        if (iter[index] != ends[index]) {
            auto value = *iter[index];
            iter[index]++;
            auto ts = std::get<0>(value.first);
            auto id = std::get<1>(value.first);
            auto offset = value.second;
            heap.push_back(std::make_tuple(ts, id, offset, index));
            std::push_heap(heap.begin(), heap.end(), std::greater<Heap::value_type>());
        }
    }
    return AKU_SUCCESS;
}

size_t Bucket::precise_count() const noexcept {
    return limit_.precise();
}

// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, size_t max_size)
    : ttl_(ttl)
    , max_size_(max_size)
    , baseline_()
    , minmax_()
{
    // We need to calculate shift width. So, we got ttl in some units
    // of measure (units of measure that akumuli doesn't know about).
    shift_ = log2(ttl.value/AKU_LIMITS_MAX_CACHES);
    if ((1 << shift_) < AKU_LIMITS_MIN_TTL) {
        throw std::runtime_error("TTL is too small");
    }
}

void Cache::update_minmax_() noexcept {
    for (Bucket* buc: ordered_buckets_) {
        auto bl = buc->baseline;
        if (bl < minmax_.first) minmax_.first = bl;
        if (bl > minmax_.second) minmax_.second = bl;
    }
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
            // bucket is not already created by another thread
            if (rel_index > AKU_LIMITS_MAX_CACHES) {
                return AKU_ELATE_WRITE;
            }
            if (rel_index < 0) {
                // Future write! Mark all outdated buckets.
                auto size = cache_.size();
                auto min_baseline = absolute_index - AKU_LIMITS_MAX_CACHES;
                for(auto& b: ordered_buckets_) {
                    if (b->baseline < min_baseline && b->state.load() == 0) {
                        b->state++;
                        *nswapped += 1;
                    }
                }
                baseline_ = absolute_index;
            }
            size_t bucket_size = sizeof(Bucket);
            Bucket* new_bucket = allocator_.allocate(bucket_size);
            allocator_.construct(new_bucket, (int64_t)max_size_, baseline_);
            cache_.insert(std::make_pair(absolute_index, new_bucket));
            ordered_buckets_.push_front(new_bucket);
            update_minmax_();
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

int Cache::pick_last(EntryOffset* offsets, size_t size, size_t* noffsets) noexcept {
    // fastpath - check all params
    if (size == 0 || offsets == nullptr)
        return AKU_EBAD_ARG;

    // get one bucket at a time under lock
    std::unique_lock<LockType> lock(lock_);
    Bucket* bucket = ordered_buckets_.back();
    *noffsets = bucket->precise_count();
    if (*noffsets > size) {
        // Buffer is to small
        return AKU_ENO_MEM;
    }
    BufferedCursor cursor(offsets, size);
    Caller caller;
    int status = bucket->merge(caller, &cursor);
    if (status == AKU_SUCCESS) {
        size_t bucket_size = sizeof(Bucket);
        ordered_buckets_.pop_back();
        TableType::accessor accessor;
        if (this->cache_.find(accessor, bucket->baseline)) {
            cache_.erase(accessor);
        }
        allocator_.destroy(bucket);
        allocator_.deallocate(bucket, bucket_size);
        update_minmax_();
    } else {
        *noffsets = 0;
    }
    return status;
}

void Cache::search(Caller& caller, InternalCursor *cur, SearchQuery& query) const noexcept {

    bool forward = query.direction == AKU_CURSOR_DIR_FORWARD;
    bool backward = query.direction == AKU_CURSOR_DIR_BACKWARD;

    if (query.upperbound < query.lowerbound
        || !(forward ^ backward)
    ) {
        // Invalid direction or timestamps
        cur->set_error(caller, AKU_EBAD_ARG);
        return;
    }

    std::vector<int64_t> indexes;
    auto tslow= query.lowerbound.value;
    auto keylow = tslow >> shift_;
    auto tshi = query.upperbound.value;
    auto keyhi = (tshi >> shift_) + AKU_LIMITS_MAX_CACHES;
    {
        std::lock_guard<LockType> gurad(lock_);
        if (keylow < minmax_.first) keylow = minmax_.first;
        if (keyhi > minmax_.second) keyhi = minmax_.second;
    }
    if (forward) {
        for(auto i = keylow; i <= keyhi; i++) {
            indexes.push_back(i);
        }
    } else {
        for(auto i = keyhi; i >= keylow; i--) {
            indexes.push_back(i);
        }
    }
    for (auto ix: indexes) {
        TableType::accessor accessor;
        if (this->cache_.find(accessor, ix)) {
            accessor->second->search(caller, cur, query);
        }
    }
    cur->complete(caller);
}

}  // namespace Akumuli
