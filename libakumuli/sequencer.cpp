/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
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

// Max space required to store one data element
#define SPACE_PER_ELEMENT 20

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

TimeSeriesValue::TimeSeriesValue(aku_Timestamp ts, aku_ParamId id, uint32_t value, uint32_t value_length)
    : key_ts_(ts)
    , key_id_(id)
    , type_(BLOB)
{
    payload.blob.value = value;
    payload.blob.value_length = value_length;
}

TimeSeriesValue::TimeSeriesValue(aku_Timestamp ts, aku_ParamId id, double value)
    : key_ts_(ts)
    , key_id_(id)
    , type_(DOUBLE)
{
    payload.value = value;
}

aku_Timestamp TimeSeriesValue::get_timestamp() const {
    return key_ts_;
}

aku_ParamId TimeSeriesValue::get_paramid() const {
    return key_id_;
}

aku_Sample TimeSeriesValue::to_result(PageHeader const *page) const {
    aku_Sample res;
    if (type_ == BLOB) {
        res.payload.type                = aku_PData::BLOB;
        res.payload.value.blob.begin    = page->read_entry_data(payload.blob.value);
        res.payload.value.blob.size     = payload.blob.value_length;
    } else {
        res.payload.type                = aku_PData::FLOAT;
        res.payload.value.float64       = payload.value;
    }
    res.paramid   = key_id_;
    res.timestamp = key_ts_;
    return res;
}

void TimeSeriesValue::add_to_header(ChunkHeader *chunk_header) const {
    chunk_header->timestamps.push_back(key_ts_);
    chunk_header->paramids.push_back(key_id_);
    if (type_ == BLOB) {
        ChunkValue chnk;
        chnk.type = ChunkValue::BLOB;
        chnk.value.blobval.offset = payload.blob.value;
        chnk.value.blobval.length = payload.blob.value_length;
        chunk_header->values.push_back(chnk);
    } else {
        ChunkValue chnk;
        chnk.type = ChunkValue::FLOAT;
        chnk.value.floatval = payload.value;
        chunk_header->values.push_back(chnk);
    }
}

bool TimeSeriesValue::is_blob() const {
    return type_ == BLOB;
}

bool operator < (TimeSeriesValue const& lhs, TimeSeriesValue const& rhs) {
    auto lhstup = std::make_tuple(lhs.key_ts_, lhs.key_id_);
    auto rhstup = std::make_tuple(rhs.key_ts_, rhs.key_id_);
    return lhstup < rhstup;
}

bool chunk_order_LT (TimeSeriesValue const& lhs, TimeSeriesValue const& rhs) {
    auto lhstup = std::make_tuple(lhs.key_id_, lhs.key_ts_);
    auto rhstup = std::make_tuple(rhs.key_id_, rhs.key_ts_);
    return lhstup < rhstup;
}

// Sequencer

Sequencer::Sequencer(PageHeader const* page, aku_Config config)
    : window_size_(config.window_size)
    , page_(page)
    , top_timestamp_()
    , checkpoint_(0u)
    , sequence_number_ {0}
    , run_locks_(RUN_LOCK_FLAGS_SIZE)
    , space_estimate_(0u)
    , c_threshold_(config.compression_threshold)
{
    key_.reset(new SortedRun());
    key_->push_back(TimeSeriesValue());
}

//! Checkpoint id = ⌊timestamp/window_size⌋
uint32_t Sequencer::get_checkpoint_(aku_Timestamp ts) const {
    // TODO: use fast integer division (libdivision or else)
    return ts / window_size_;
}

//! Convert checkpoint id to timestamp
aku_Timestamp Sequencer::get_timestamp_(uint32_t cp) const {
    return cp*window_size_;
}

// move sorted runs to ready_ collection
int Sequencer::make_checkpoint_(uint32_t new_checkpoint) {
    int flag = sequence_number_.fetch_add(1) + 1;
    if (flag % 2 != 0) {
        auto old_top = get_timestamp_(checkpoint_);
        checkpoint_ = new_checkpoint;
        vector<PSortedRun> new_runs;
        for (auto& sorted_run: runs_) {
            auto it = lower_bound(sorted_run->begin(), sorted_run->end(), TimeSeriesValue(old_top, AKU_LIMITS_MAX_ID, 0u, 0u));
            // Check that compression threshold is reached
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
        space_estimate_ = 0u;
        for (auto& sorted_run: new_runs) {
            space_estimate_ += sorted_run->size() * SPACE_PER_ELEMENT;
        }
        swap(runs_, new_runs);

        size_t ready_size = 0u;
        for (auto& sorted_run: ready_) {
            ready_size += sorted_run->size();
        }
        if (ready_size < c_threshold_) {
            // If ready doesn't contains enough data compression wouldn't be efficient,
            //  we need to wait for more data to come
            flag = sequence_number_.fetch_add(1) + 1;
            // We should make sorted runs in ready_ array searchable again
            for (auto& sorted_run: ready_) {
                runs_.push_back(sorted_run);
            }
            ready_.clear();
        }
    }
    return flag;
}

/** Check timestamp and make checkpoint if timestamp is large enough.
  * @returns error code and flag that indicates whether or not new checkpoint is created
  */
std::tuple<int, int> Sequencer::check_timestamp_(aku_Timestamp ts) {
    int error_code = AKU_SUCCESS;
    if (ts < top_timestamp_) {
        auto delta = top_timestamp_ - ts;
        if (delta > window_size_) {
            error_code = AKU_ELATE_WRITE;
        }
        return make_tuple(error_code, 0);
    }
    auto point = get_checkpoint_(ts);
    int flag = 0;
    if (point > checkpoint_) {
        // Create new checkpoint
        flag = make_checkpoint_(point);
        if (flag % 2 == 0) {
            // Previous checkpoint not completed
            error_code = AKU_EBUSY;
        }
    }
    top_timestamp_ = ts;
    return make_tuple(error_code, flag);
}

std::tuple<int, int> Sequencer::add(TimeSeriesValue const& value) {
    // FIXME: max_cache_size_ is not used
    int status = 0;
    int lock = 0;
    tie(status, lock) = check_timestamp_(value.get_timestamp());
    if (status != AKU_SUCCESS) {
        return make_tuple(status, lock);
    }

    key_->pop_back();
    key_->push_back(value);

    Lock guard(runs_resize_lock_);
    space_estimate_ += SPACE_PER_ELEMENT;
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
    return make_tuple(AKU_SUCCESS, lock);
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

aku_Status Sequencer::close(PageHeader* target) {
    wrlock_all(run_locks_);
    for (auto& sorted_run: runs_) {
        ready_.push_back(move(sorted_run));
    }
    unlock_all(run_locks_);

    runs_resize_lock_.lock();
    runs_.clear();
    runs_resize_lock_.unlock();

    sequence_number_.store(1);
    return merge_and_compress(target);
}

int Sequencer::reset() {
    wrlock_all(run_locks_);
    for (auto& sorted_run: runs_) {
        ready_.push_back(move(sorted_run));
    }
    unlock_all(run_locks_);

    runs_resize_lock_.lock();
    runs_.clear();
    runs_resize_lock_.unlock();
    sequence_number_.store(1);
    return 1;
}

template<class TKey, int dir>
struct TimeOrderMergePredicate;

template<class TKey>
struct TimeOrderMergePredicate<TKey, AKU_CURSOR_DIR_FORWARD> {
    greater<TKey> greater_;
    bool operator () (TKey const& lhs, TKey const& rhs) const {
        return greater_(lhs, rhs);
    }
};

template<class TKey>
struct TimeOrderMergePredicate<TKey, AKU_CURSOR_DIR_BACKWARD> {
    less<TKey> less_;
    bool operator () (TKey const& lhs, TKey const& rhs) const {
        return less_(lhs, rhs);
    }
};

template<class TKey, int Dir>
struct ChunkOrderMergePredicate;

template<class TKey>
struct ChunkOrderMergePredicate<std::tuple<TKey, int>, AKU_CURSOR_DIR_FORWARD> {
    // Only forward chunk-order merge is allowed
    bool operator () (std::tuple<TKey, int> const& lhs, std::tuple<TKey, int> const& rhs) const {
        return chunk_order_LT(std::get<0>(lhs), std::get<0>(rhs));
    }
};

template<class TRun, int dir>
struct RunIter;

template<class TRun>
struct RunIter<std::shared_ptr<TRun>, AKU_CURSOR_DIR_FORWARD> {
    typedef boost::iterator_range<typename TRun::const_iterator> range_type;
    typedef typename TRun::value_type value_type;
    static range_type make_range(std::shared_ptr<TRun> const& run) {
        return boost::make_iterator_range(run->begin(), run->end());
    }
};

template<class TRun>
struct RunIter<std::shared_ptr<TRun>, AKU_CURSOR_DIR_BACKWARD> {
    typedef boost::iterator_range<typename TRun::const_reverse_iterator> range_type;
    typedef typename TRun::value_type value_type;
    static range_type make_range(std::shared_ptr<TRun> const& run) {
        return boost::make_iterator_range(run->rbegin(), run->rend());
    }
};

/** Merge sequences and push it to consumer */
template <
        //! Merge predicate for time-series values
        template<class PredKey, int PredDirection> class Predicate,
        int dir,
        class Consumer
>
void kway_merge(vector<Sequencer::PSortedRun> const& runs, Consumer& cons) {
    typedef RunIter<Sequencer::PSortedRun, dir> RIter;
    typedef typename RIter::range_type range_t;
    typedef typename RIter::value_type KeyType;
    std::vector<range_t> ranges;
    for(auto i = runs.begin(); i != runs.end(); i++) {
        ranges.push_back(RIter::make_range(*i));
    }

    typedef tuple<KeyType, int> HeapItem;
    typedef Predicate<HeapItem, dir> Comp;
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

void Sequencer::merge(Caller& caller, InternalCursor* cur) {
    bool owns_lock = sequence_number_.load() % 2;  // progress_flag_ must be odd to start
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
        aku_Sample result = val.to_result(page);
        return cur->put(caller, result);
    };

    kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_FORWARD>(ready_, consumer);

    ready_.clear();
    cur->complete(caller);

    sequence_number_.fetch_add(1);  // progress_flag_ is even again
}

aku_Status Sequencer::merge_and_compress(PageHeader* target) {
    bool owns_lock = sequence_number_.load() % 2;  // progress_flag_ must be odd to start
    if (!owns_lock) {
        return AKU_EBUSY;
    }
    if (ready_.size() == 0) {
        return AKU_ENO_DATA;
    }

    ChunkHeader chunk_header;

    auto consumer = [&](TimeSeriesValue const& val) {
        val.add_to_header(&chunk_header);
        return true;
    };

    kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_FORWARD>(ready_, consumer);
    ready_.clear();

    ChunkHeader reindexed_header;
    if (!CompressionUtil::convert_from_time_order(chunk_header, &reindexed_header)) {
        AKU_PANIC("Invalid chunk");
    }

    auto status = target->complete_chunk(reindexed_header);
    if (status != AKU_SUCCESS) {
        return status;
    }
    sequence_number_.fetch_add(1);  // progress_flag_ is even again
    return AKU_SUCCESS;
}

std::tuple<aku_Timestamp, int> Sequencer::get_window() const {
    return std::make_tuple(top_timestamp_ > window_size_ ? top_timestamp_ - window_size_
                                                         : top_timestamp_,
                           sequence_number_.load());
}

uint32_t Sequencer::get_space_estimate() const {
    // ready_ must be empty here
    return space_estimate_ + SPACE_PER_ELEMENT;
}

struct SearchPredicate {
    SearchQuery const& query;
    SearchPredicate(SearchQuery const& q) : query(q) {}

    bool operator () (TimeSeriesValue const& value) const {
        if (query.lowerbound <= value.get_timestamp() &&
            query.upperbound >= value.get_timestamp())
        {
            if (query.param_pred(value.get_paramid()) == SearchQuery::MATCH) {
                return true;
            }
        }
        return false;
    }
};

void Sequencer::filter(PSortedRun run, SearchQuery const& q, std::vector<PSortedRun>* results) const {
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

void Sequencer::search(Caller& caller, InternalCursor* cur, SearchQuery query, int sequence_number) const {
    int seq_id = sequence_number_.load();
    if (seq_id % 2 != 0 || sequence_number != seq_id) {
        cur->set_error(caller, AKU_EBUSY);
        return;
    }
    std::vector<PSortedRun> filtered;
    std::vector<PSortedRun> pruns;
    Lock runs_guard(runs_resize_lock_);
    pruns = runs_;
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
        aku_Sample result = val.to_result(page);
        return cur->put(caller, result);
    };

    if (query.direction == AKU_CURSOR_DIR_FORWARD) {
        kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_FORWARD>(filtered, consumer);
    } else {
        kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_BACKWARD>(filtered, consumer);
    }

    if (seq_id != sequence_number_.load()) {
        cur->set_error(caller, AKU_EBUSY);
        return;
    } else {
        cur->complete(caller);
    }
}

void Sequencer::filterV2(PSortedRun run, std::shared_ptr<QP::IQueryProcessor> q, std::vector<PSortedRun>* results) const {
    if (run->empty()) {
        return;
    }
    PSortedRun result(new SortedRun);
    auto lkey = TimeSeriesValue(q->lowerbound(), 0u, 0u, 0u);
    auto rkey = TimeSeriesValue(q->upperbound(), ~0u, 0u, 0u);
    auto begin = std::lower_bound(run->begin(), run->end(), lkey);
    auto end = std::upper_bound(run->begin(), run->end(), rkey);
    std::copy(begin, end, std::back_inserter(*result));
    results->push_back(move(result));
}

void Sequencer::searchV2(std::shared_ptr<QP::IQueryProcessor> query, int sequence_number) const {
    int seq_id = sequence_number_.load();
    if (seq_id % 2 != 0 || sequence_number != seq_id) {
        query->set_error(AKU_EBUSY);
        return;
    }
    std::vector<PSortedRun> filtered;
    std::vector<PSortedRun> pruns;
    Lock runs_guard(runs_resize_lock_);
    pruns = runs_;
    runs_guard.unlock();
    int run_ix = 0;
    for (auto const& run: pruns) {
        auto ix = run_ix & RUN_LOCK_FLAGS_MASK;
        auto& rwlock = run_locks_.at(ix);
        rwlock.rdlock();
        filterV2(run, query, &filtered);
        rwlock.unlock();
        run_ix++;
    }

    auto page = page_;
    auto consumer = [query, page](TimeSeriesValue const& val) {
        aku_Sample result = val.to_result(page);
        return query->put(result);
    };

    if (query->direction() == AKU_CURSOR_DIR_FORWARD) {
        kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_FORWARD>(filtered, consumer);
    } else {
        kway_merge<TimeOrderMergePredicate, AKU_CURSOR_DIR_BACKWARD>(filtered, consumer);
    }

    if (seq_id != sequence_number_.load()) {
        query->set_error(AKU_EBUSY);
        return;
    } else {
        query->stop();
    }
}

}  // namespace Akumuli
