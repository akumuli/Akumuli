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

using namespace std;

namespace Akumuli {

template<typename RunType>
bool top_element_less(const RunType& x, const RunType& y)
{
    return x.back() < y.back();
}

template<typename RunType>
bool top_element_more(const RunType& x, const RunType& y)
{
    return top_element_less(y, x);
}

TimeSeriesValue::TimeSeriesValue() {}

TimeSeriesValue::TimeSeriesValue(TimeStamp ts, ParamId id, EntryOffset offset)
    : key_(ts, id)
    , value(offset)
{
}

bool operator < (TimeSeriesValue const& lhs, TimeSeriesValue const& rhs) {
    return lhs.key_ < rhs.key_;
}

// Sequencer

Sequencer::Sequencer(PageHeader const* page, TimeDuration window_size)
    : window_size_(window_size)
    , page_(page)
    , top_timestamp_()
    , checkpoint_(0u)
{
    if (window_size.value <= 0) {
        throw runtime_error("window size must greather than zero");
    }
    key_.push_back(TimeSeriesValue());
}

//! Checkpoint id = ⌊timestamp/window_size⌋
uint32_t Sequencer::get_checkpoint_(TimeStamp ts) const noexcept {
    // TODO: use fast integer division (libdivision or else)
    return ts.value / window_size_.value;
}

//! Convert checkpoint id to timestamp
TimeStamp Sequencer::get_timestamp_(uint32_t cp) const noexcept {
    return TimeStamp::make(cp*window_size_.value);
}

// move sorted runs to ready_ collection
bool Sequencer::make_checkpoint_(uint32_t new_checkpoint) noexcept {
    if(!progress_flag_.try_lock()) {
        return false;
    }
    auto old_top = get_timestamp_(checkpoint_);
    checkpoint_ = new_checkpoint;
    if (!ready_.empty()) {
        throw runtime_error("sequencer invariant is broken");
    }
    vector<SortedRun> new_runs;
    for (auto& sorted_run: runs_) {
        auto it = lower_bound(sorted_run.begin(), sorted_run.end(), TimeSeriesValue(old_top, AKU_LIMITS_MAX_ID, 0));
        if (it == sorted_run.begin()) {
            // all timestamps are newer than old_top, do nothing
            new_runs.push_back(move(sorted_run));
            continue;
        } else if (it == sorted_run.end()) {
            // all timestamps are older than old_top, move them
            ready_.push_back(move(sorted_run));
        } else {
            // it is in between of the sorted run - split
            SortedRun run;
            copy(sorted_run.begin(), it, back_inserter(run));  // copy old
            ready_.push_back(move(run));
            run.clear();
            copy(it, sorted_run.end(), back_inserter(run));  // copy new
            new_runs.push_back(move(run));
        }
    }
    swap(runs_, new_runs);
    atomic_thread_fence(memory_order_acq_rel);
    return true;
}

/** Check timestamp and make checkpoint if timestamp is large enough.
  * @returns error code and flag that indicates whether or not new checkpoint is created
  */
tuple<int, bool> Sequencer::check_timestamp_(TimeStamp ts) noexcept {
    int error_code = AKU_SUCCESS;
    if (ts < top_timestamp_) {
        auto delta = top_timestamp_ - ts;
        if (delta.value > window_size_.value) {
            error_code = AKU_ELATE_WRITE;
        }
        return make_tuple(error_code, false);
    }
    bool new_cp = false;
    auto point = get_checkpoint_(ts);
    if (point > checkpoint_) {
        // Create new checkpoint
        new_cp = make_checkpoint_(point);
        if (!new_cp) {
            // Previous checkpoint not completed
            error_code = AKU_EBUSY;
        }
    }
    top_timestamp_ = ts;
    return make_tuple(error_code, new_cp);
}

tuple<int, bool> Sequencer::add(TimeSeriesValue const& value) {
    int status;
    bool new_checkpoint;
    tie(status, new_checkpoint) = check_timestamp_(get<0>(value.key_));
    if (status != AKU_SUCCESS) {
        return make_tuple(status, new_checkpoint);
    }
    key_.pop_back();
    key_.push_back(value);
    auto begin = runs_.begin();
    auto end = runs_.end();
    auto insert_it = lower_bound(begin, end, key_, top_element_more<SortedRun>);
    if (insert_it == runs_.end()) {
        SortedRun new_pile;
        new_pile.push_back(value);
        runs_.push_back(new_pile);
    } else {
        insert_it->push_back(value);
    }
    return make_tuple(AKU_SUCCESS, new_checkpoint);
}

bool Sequencer::close() {
    if (!progress_flag_.try_lock()) {
        return false;
    }
    if (!ready_.empty()) {
        throw runtime_error("sequencer invariant is broken");
    }
    for (auto& sorted_run: runs_) {
        ready_.push_back(move(sorted_run));
    }
    runs_.clear();
    atomic_thread_fence(memory_order_acq_rel);
    return true;
}

static void kway_merge(vector<SortedRun> const& runs, Caller& caller, InternalCursor* out_iter) noexcept {
    size_t n = runs.size();
    typedef typename SortedRun::const_iterator iter_t;
    iter_t iter[n], ends[n];
    int cnt = 0;
    for(auto i = runs.begin(); i != runs.end(); i++) {
        iter[cnt] = i->begin();
        ends[cnt] = i->end();
        cnt++;
    }

    typedef tuple<TimeSeriesValue, int> HeapItem;
    typedef vector<HeapItem> Heap;
    Heap heap;

    for(auto index = 0u; index < n; index++) {
        if (iter[index] != ends[index]) {
            auto value = *iter[index];
            iter[index]++;
            heap.push_back(make_tuple(value, index));
        }
    }

    make_heap(heap.begin(), heap.end(), greater<HeapItem>());

    while(!heap.empty()) {
        pop_heap(heap.begin(), heap.end(), greater<HeapItem>());
        auto item = heap.back();
        auto point = get<0>(item);
        int index = get<1>(item);
        out_iter->put(caller, point.value, page_);
        heap.pop_back();
        if (iter[index] != ends[index]) {
            auto point = *iter[index];
            iter[index]++;
            heap.push_back(make_tuple(point, index));
            push_heap(heap.begin(), heap.end(), greater<HeapItem>());
        }
    }
}

void Sequencer::merge(Caller& caller, InternalCursor* out_iter) noexcept {
    bool false_if_ok = progress_flag_.try_lock();
    if (!false_if_ok) {
        // Error! Merge called too early
        out_iter->set_error(caller, AKU_EBUSY);
        return;
    }

    if (ready_.size() == 0) {
        // Things go crazy
        out_iter->set_error(caller, AKU_ENO_DATA);
        return;
    }

    kway_merge(ready_, caller, cur);

    // Sequencer invariant - if progress_flag_ is unset - ready_ flag must be empty
    // we've got only one place to store ready to sync data, if such data is present
    // progress_flag_ must be set (it indicates that merge/sync procedure is in progress)
    // after that we must clear ready_ collection and free some space for new data, after
    // that progress_flag_ can be cleared.

    ready_.clear();
    atomic_thread_fence(memory_order_acq_rel);
    progress_flag_.unlock();
    out_iter->complete(caller);
}

void Sequencer::lock_run(int ix) const noexcept {
   bool prev = run_lock_flags_[RUN_LOCK_FLAGS_MASK & ix].test_and_set();
   // TODO: busy wait with exp backoff
}

void Sequencer::search(Caller& caller, InternalCursor* cur, SeqrchQuery const& query) const noexcept {
    std::lock_guard<std::mutex> guard(progress_flag_);
    // we can get here only before checkpoint (or after merge was completed)
    // that means that ready_ is empty
    assert(ready_.empty());
    std::vector<SortedRun> filtered;
    for (const auto& run: runs_) {
        lock_run(run);
        filter(run, query, filtered);
        unlock_run(run);
    }
    kway_merge(filtered, caller, cur);
}

// Old stuff

// This method must be called from the same thread
int Sequence::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    auto key = make_tuple(ts, param);

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

void Sequence::search(Caller& caller, InternalCursor* cursor, SearchQuery const& query, PageHeader *page) const noexcept {

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
                cursor->put(caller, it_upper->second, page);
            }
        }

        if (it_lower != data_.end()) {
            if (match(it_lower)) {
                cursor->put(caller, it_lower->second, page);
            }
        }
    }
    else
    {
        for(; it_lower != it_upper; it_lower++) {
            if (match(it_lower)) {
                cursor->put(caller, it_lower->second, page);
            }
        }
    }
}

size_t Sequence::size() const noexcept {
    return data_.size();
}

void Sequence::get_all(Caller& caller, InternalCursor* cursor, PageHeader* page) const noexcept {
    for(auto i = data_.begin(); i != data_.end(); i++) {
        cursor->put(caller, i->second, page);
    }
}

Sequence::MapType::const_iterator Sequence::begin() const {
    return data_.begin();
}

Sequence::MapType::const_iterator Sequence::end() const {
    return data_.end();
}


// Bucket -------------------------------------

Bucket::Bucket(int64_t size_limit, int64_t baseline)
    : limit_(size_limit)
    , baseline(baseline)
    , state(0)
{
}

int Bucket::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    if (limit_.dec()) {
        return seq_.local().add(ts, param, offset);
    }
    return AKU_EOVERFLOW;
}

void Bucket::search(Caller &caller, InternalCursor* cursor, SearchQuery const& query, PageHeader *page) const noexcept {
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
    seq->search(caller, cursor, query, page);
}

typedef Sequence::MapType::const_iterator iter_t;

int Bucket::merge(Caller& caller, InternalCursor *cur, PageHeader* page) const noexcept {
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

    for(auto index = 0u; index < n; index++) {
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
        cur->put(caller, offset, page);
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

Cache::Cache(TimeDuration ttl, size_t max_size, PageHeader* page)
    : baseline_()
    , ttl_(ttl)
    , max_size_(max_size)
    , minmax_()
    , page_(page)
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

// TODO: use cursor here
int Cache::pick_last(CursorResult *offsets, size_t size, size_t* noffsets) noexcept {
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
    int status = bucket->merge(caller, &cursor, page_);
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
            accessor->second->search(caller, cur, query, page_);
        }
    }
    cur->complete(caller);
}

}  // namespace Akumuli
