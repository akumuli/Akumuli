/**
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "akumuli_def.h"
#include "sequencer.h"
#include "util.h"
#include "compression.h"

#include <thread>
#include <boost/heap/skew_heap.hpp>
#include <boost/range.hpp>
#include <boost/range/iterator_range.hpp>

#define PARAM_ID 1
#define TIMESTMP 0

using namespace std;

namespace Akumuli {

template<typename RunType>
bool top_element_less(const RunType& x, const RunType& y)
{
    return x->back() < y->back();
}

template<typename RunType>
bool top_element_more(const RunType& x, const RunType& y)
{
    return top_element_less(y, x);
}

TimeSeriesValue::TimeSeriesValue() {}

TimeSeriesValue::TimeSeriesValue(aku_TimeStamp ts, aku_ParamId id, aku_EntryOffset offset, uint32_t value_length)
    : key_(ts, id)
    , value(offset)
    , value_length(value_length)
{
}

aku_TimeStamp TimeSeriesValue::get_timestamp() const {
    return std::get<TIMESTMP>(key_);
}

aku_ParamId TimeSeriesValue::get_paramid() const {
    return std::get<PARAM_ID>(key_);
}

bool operator < (TimeSeriesValue const& lhs, TimeSeriesValue const& rhs) {
    return lhs.key_ < rhs.key_;
}

// Sequencer

Sequencer::Sequencer(PageHeader const* page, aku_Duration window_size)
    : window_size_(window_size)
    , page_(page)
    , top_timestamp_()
    , checkpoint_(0u)
    , run_locks_(RUN_LOCK_FLAGS_SIZE)
{
    key_.reset(new SortedRun());
    key_->push_back(TimeSeriesValue());
}

//! Checkpoint id = ⌊timestamp/window_size⌋
uint32_t Sequencer::get_checkpoint_(aku_TimeStamp ts) const {
    // TODO: use fast integer division (libdivision or else)
    return ts / window_size_;
}

//! Convert checkpoint id to timestamp
aku_TimeStamp Sequencer::get_timestamp_(uint32_t cp) const {
    return cp*window_size_;
}

// move sorted runs to ready_ collection
void Sequencer::make_checkpoint_(uint32_t new_checkpoint, Lock& lock) {
    if(!lock.try_lock()) {
        return;
    }
    auto old_top = get_timestamp_(checkpoint_);
    checkpoint_ = new_checkpoint;
    if (!ready_.empty()) {
        throw runtime_error("sequencer invariant is broken");
    }
    vector<PSortedRun> new_runs;
    for (auto& sorted_run: runs_) {
        auto it = lower_bound(sorted_run->begin(), sorted_run->end(), TimeSeriesValue(old_top, AKU_LIMITS_MAX_ID, 0u, 0u));
        if (it == sorted_run->begin()) {
            // all timestamps are newer than old_top, do nothing
            new_runs.push_back(move(sorted_run));
            continue;
        } else if (it == sorted_run->end()) {
            // all timestamps are older than old_top, move them
            ready_.push_back(move(sorted_run));
        } else {
            // it is in between of the sorted run - split
            PSortedRun run(new SortedRun());
            copy(sorted_run->begin(), it, back_inserter(*run));  // copy old
            ready_.push_back(move(run));
            run.reset(new SortedRun());
            copy(it, sorted_run->end(), back_inserter(*run));  // copy new
            new_runs.push_back(move(run));
        }
    }
    Lock guard(runs_resize_lock_);
    swap(runs_, new_runs);
}

/** Check timestamp and make checkpoint if timestamp is large enough.
  * @returns error code and flag that indicates whether or not new checkpoint is created
  */
int Sequencer::check_timestamp_(aku_TimeStamp ts, Lock& lock) {
    int error_code = AKU_SUCCESS;
    if (ts < top_timestamp_) {
        auto delta = top_timestamp_ - ts;
        if (delta > window_size_) {
            error_code = AKU_ELATE_WRITE;
        }
        return error_code;
    }
    auto point = get_checkpoint_(ts);
    if (point > checkpoint_) {
        // Create new checkpoint
        make_checkpoint_(point, lock);
        if (!lock.owns_lock()) {
            // Previous checkpoint not completed
            error_code = AKU_EBUSY;
        }
    }
    top_timestamp_ = ts;
    return error_code;
}

std::tuple<int, Sequencer::Lock> Sequencer::add(TimeSeriesValue const& value) {
    // FIXME: max_cache_size_ is not used
    int status;
    Lock lock(progress_flag_, defer_lock);
    status = check_timestamp_(get<0>(value.key_), lock);
    if (status != AKU_SUCCESS) {
        return make_tuple(status, move(lock));
    }

    key_->pop_back();
    key_->push_back(value);

    Lock guard(runs_resize_lock_);
    auto begin = runs_.begin();
    auto end = runs_.end();
    auto insert_it = lower_bound(begin, end, key_, top_element_more<PSortedRun>);
    int run_ix = distance(begin, insert_it);
    bool new_run_needed = insert_it == runs_.end();
    SortedRun* run = nullptr;
    if (!new_run_needed) {
        run = insert_it->get();
    }
    guard.unlock();

    if (!new_run_needed) {
        auto ix = run_ix & RUN_LOCK_FLAGS_MASK;
        auto& rwlock = run_locks_.at(ix);
        rwlock.wrlock();
        run->push_back(value);
        rwlock.unlock();
    } else {
        guard.lock();
        PSortedRun new_pile(new SortedRun());
        new_pile->push_back(value);
        runs_.push_back(move(new_pile));
        guard.unlock();
    }
    return make_tuple(AKU_SUCCESS, move(lock));
}

template<class Cont>
void wrlock_all(Cont& cont) {
    for (auto& rwlock: cont) {
        rwlock.wrlock();
    }
}

template<class Cont>
void unlock_all(Cont& cont) {
    for (auto& rwlock: cont) {
        rwlock.unlock();
    }
}

Sequencer::Lock Sequencer::close() {
    Lock lock(progress_flag_, defer_lock);
    if (!lock.try_lock()) {
        return move(lock);
    }
    if (!ready_.empty()) {
        throw runtime_error("sequencer invariant is broken");
    }

    wrlock_all(run_locks_);
    for (auto& sorted_run: runs_) {
        ready_.push_back(move(sorted_run));
    }
    unlock_all(run_locks_);

    runs_resize_lock_.lock();
    runs_.clear();
    runs_resize_lock_.unlock();

    return move(lock);
}

template<class TKey, int dir>
struct MergePred;

template<class TKey>
struct MergePred<TKey, AKU_CURSOR_DIR_FORWARD> {
    greater<TKey> greater_;
    bool operator () (TKey const& lhs, TKey const& rhs) const {
        return greater_(lhs, rhs);
    }
};

template<class TKey>
struct MergePred<TKey, AKU_CURSOR_DIR_BACKWARD> {
    less<TKey> less_;
    bool operator () (TKey const& lhs, TKey const& rhs) const {
        return less_(lhs, rhs);
    }
};

template<class TRun, int dir>
struct RunIter;

template<class TRun>
struct RunIter<std::unique_ptr<TRun>, AKU_CURSOR_DIR_FORWARD> {
    typedef boost::iterator_range<typename TRun::const_iterator> range_type;
    typedef typename TRun::value_type value_type;
    static range_type make_range(std::unique_ptr<TRun> const& run) {
        return boost::make_iterator_range(run->begin(), run->end());
    }
};

template<class TRun>
struct RunIter<std::unique_ptr<TRun>, AKU_CURSOR_DIR_BACKWARD> {
    typedef boost::iterator_range<typename TRun::const_reverse_iterator> range_type;
    typedef typename TRun::value_type value_type;
    static range_type make_range(std::unique_ptr<TRun> const& run) {
        return boost::make_iterator_range(run->rbegin(), run->rend());
    }
};

/** Merge sequences and push it to consumer */
template <int dir, class Consumer>
void kway_merge(vector<Sequencer::PSortedRun> const& runs, Consumer& cons) {
    typedef RunIter<Sequencer::PSortedRun, dir> RIter;
    typedef typename RIter::range_type range_t;
    typedef typename RIter::value_type KeyType;
    std::vector<range_t> ranges;
    for(auto i = runs.begin(); i != runs.end(); i++) {
        ranges.push_back(RIter::make_range(*i));
    }

    typedef tuple<KeyType, int> HeapItem;
    typedef MergePred<HeapItem, dir> Comp;
    typedef boost::heap::skew_heap<HeapItem, boost::heap::compare<Comp>> Heap;
    Heap heap;

    int index = 0;
    for(auto& range: ranges) {
        if (!range.empty()) {
            KeyType value = range.front();
            range.advance_begin(1);
            heap.push(make_tuple(value, index));
        }
        index++;
    }

    while(!heap.empty()) {
        HeapItem item = heap.top();
        KeyType point = get<0>(item);
        int index = get<1>(item);
        if (!cons(point)) {
            // Interrupted
            return;
        }
        heap.pop();
        if (!ranges[index].empty()) {
            KeyType point = ranges[index].front();
            ranges[index].advance_begin(1);
            heap.push(make_tuple(point, index));
        }
    }
}

void Sequencer::merge(Caller& caller, InternalCursor* cur, Lock&& lock) {
    bool owns_lock = lock.owns_lock();
    if (!owns_lock) {
        // Error! Merge called too early
        cur->set_error(caller, AKU_EBUSY);
        return;
    }

    if (ready_.size() == 0) {
        // Things go crazy
        cur->set_error(caller, AKU_ENO_DATA);
        return;
    }

    auto page = page_;
    auto consumer = [&caller, cur, page](TimeSeriesValue const& val) {
        return cur->put(caller, val.value, page);
    };

    wrlock_all(run_locks_);
    kway_merge<AKU_CURSOR_DIR_FORWARD>(ready_, consumer);
    unlock_all(run_locks_);
    // Sequencer invariant - if progress_flag_ is unset - ready_ flag must be empty
    // we've got only one place to store ready to sync data, if such data is present
    // progress_flag_ must be set (it indicates that merge/sync procedure is in progress)
    // after that we must clear ready_ collection and free some space for new data, after
    // that progress_flag_ can be cleared.

    ready_.clear();
    cur->complete(caller);
}

// Time stamps (sorted) -> Delta -> RLE -> Base128
typedef Base128StreamWriter<uint64_t> __Base128TSWriter;
typedef RLEStreamWriter<__Base128TSWriter, uint64_t> __RLETSWriter;
typedef DeltaStreamWriter<__RLETSWriter, uint64_t> DeltaRLETSWriter;

// ParamId -> Base128
typedef Base128StreamWriter<uint32_t> Base128IdWriter;

// Length -> RLE -> Base128
typedef Base128StreamWriter<uint32_t> __Base128LenWriter;
typedef RLEStreamWriter<__Base128LenWriter, uint32_t> RLELenWriter;

// Offset -> Base128
typedef Base128StreamWriter<uint32_t> Base128OffWriter;

void Sequencer::merge_and_compress(Sequencer::Lock&& lock) {
    if (!lock.owns_lock()) {
        // TODO: error
        return;
    }
    if (ready_.size() == 0) {
        // TODO: error
        return;
    }

    ByteVector timestamps;
    ByteVector paramids;
    ByteVector offsets;
    ByteVector lengths;

    DeltaRLETSWriter timestamp_stream(timestamps);
    Base128IdWriter paramid_stream(paramids);
    Base128OffWriter offset_stream(offsets);
    RLELenWriter length_stream(lengths);

    auto consumer = [&](TimeSeriesValue const& val) {
        auto ts = val.get_timestamp();
        auto id = val.get_paramid();
        timestamp_stream.put(ts);
        paramid_stream.put(id);
        offset_stream.put(val.value);
        length_stream.put(val.value_length);
        return true;
    };
    wrlock_all(run_locks_);
    kway_merge<AKU_CURSOR_DIR_FORWARD>(ready_, consumer);
    unlock_all(run_locks_);
}

aku_TimeStamp Sequencer::get_window() const {
    return top_timestamp_ - window_size_;
}

struct SearchPredicate {
    SearchQuery const& query;
    SearchPredicate(SearchQuery const& q) : query(q) {}

    bool operator () (TimeSeriesValue const& value) const {
        if (query.lowerbound <= get<TIMESTMP>(value.key_) &&
            query.upperbound >= get<TIMESTMP>(value.key_))
        {
            if (query.param_pred(get<PARAM_ID>(value.key_)) == SearchQuery::MATCH) {
                return true;
            }
        }
        return false;
    }
};

void Sequencer::filter(SortedRun const* run, SearchQuery const& q, std::vector<PSortedRun>* results) const {
    if (run->empty()) {
        return;
    }
    SearchPredicate search_pred(q);
    PSortedRun result(new SortedRun);
    auto lkey = TimeSeriesValue(q.lowerbound, 0u, 0u, 0u);
    auto rkey = TimeSeriesValue(q.upperbound, ~0u, 0u, 0u);
    auto begin = std::lower_bound(run->begin(), run->end(), lkey);
    auto end = std::upper_bound(run->begin(), run->end(), rkey);
    copy_if(begin, end, std::back_inserter(*result), search_pred);
    results->push_back(move(result));
}

void Sequencer::search(Caller& caller, InternalCursor* cur, SearchQuery query) const {
    std::vector<PSortedRun> filtered;
    std::vector<SortedRun const*> pruns;
    Lock runs_guard(runs_resize_lock_);
    std::transform(runs_.begin(), runs_.end(),
                   std::back_inserter(pruns),
                   [](PSortedRun const& r) {return r.get();}
    );
    runs_guard.unlock();
    int run_ix = 0;
    for (auto const& run: pruns) {
        auto ix = run_ix & RUN_LOCK_FLAGS_MASK;
        auto& rwlock = run_locks_.at(ix);
        rwlock.rdlock();
        filter(run, query, &filtered);
        rwlock.unlock();
        run_ix++;
    }

    auto page = page_;
    auto consumer = [&caller, cur, page](TimeSeriesValue const& val) {
        return cur->put(caller, val.value, page);
    };

    if (query.direction == AKU_CURSOR_DIR_FORWARD) {
        kway_merge<AKU_CURSOR_DIR_FORWARD>(filtered, consumer);
    } else {
        kway_merge<AKU_CURSOR_DIR_BACKWARD>(filtered, consumer);
    }
    cur->complete(caller);
}
}  // namespace Akumuli
