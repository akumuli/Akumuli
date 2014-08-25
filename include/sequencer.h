/**
 * PRIVATE HEADER
 *
 * Data structures for main memory storage.
 *
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


#pragma once
#include "page.h"
#include "cursor.h"

#include <tuple>
#include <vector>
#include <algorithm>
#include <memory>
#include <mutex>

namespace Akumuli {

struct TimeSeriesValue {
    std::tuple<aku_TimeStamp, aku_ParamId> key_;
    aku_EntryOffset value;

    TimeSeriesValue();

    TimeSeriesValue(aku_TimeStamp ts, aku_ParamId id, aku_EntryOffset offset);

    aku_TimeStamp get_timestamp() const;

    friend bool operator < (TimeSeriesValue const& lhs, TimeSeriesValue const& rhs);
};


/** Time-series sequencer.
  * @brief Akumuli can accept unordered time-series (this is the case when
  * clocks of the different time-series sources are slightly out of sync).
  * This component accepts all of them, filter out late writes and reorder
  * all the remaining samples by timestamp and parameter id.
  */
struct Sequencer {
    typedef std::vector<TimeSeriesValue> SortedRun;
    typedef std::unique_ptr<SortedRun>   PSortedRun;
    typedef std::mutex                   Mutex;
    typedef std::unique_lock<Mutex>      Lock;

    static const int RUN_LOCK_MAX_BACKOFF = 0x100;
    static const int RUN_LOCK_BUSY_COUNT = 0xFFF;
    static const int RUN_LOCK_FLAGS_MASK = 0x0FF;
    static const int RUN_LOCK_FLAGS_SIZE = 0x100;

    std::vector<PSortedRun>      runs_;           //< Active sorted runs
    std::vector<PSortedRun>      ready_;          //< Ready to merge
    PSortedRun                   key_;
    const aku_Duration           window_size_;
    const PageHeader* const      page_;
    aku_TimeStamp                top_timestamp_;  //< Largest timestamp ever seen
    uint32_t                     checkpoint_;     //< Last checkpoint timestamp
    mutable Mutex                progress_flag_;
    mutable Mutex                runs_resize_lock_;
    mutable std::vector<RWLock>  run_locks_;

    Sequencer(PageHeader const* page, aku_Duration window_size);

    /** Add new sample to sequence.
      * @brief Timestamp of the sample can be out of order.
      * @returns error code and flag that indicates whether of not new checkpoint is createf
      */
    std::tuple<int, Lock> add(TimeSeriesValue const& value);

    void merge(Caller& caller, InternalCursor* cur, Lock&& lock);

    Lock close();

    // Searching
    void search(Caller& caller, InternalCursor* cur, SearchQuery query) const;

    aku_TimeStamp get_window() const;

private:
    //! Checkpoint id = ⌊timestamp/window_size⌋
    uint32_t get_checkpoint_(aku_TimeStamp ts) const;

    //! Convert checkpoint id to timestamp
    aku_TimeStamp get_timestamp_(uint32_t cp) const;

    // move sorted runs to ready_ collection
    void make_checkpoint_(uint32_t new_checkpoint, Lock& lock);

    /** Check timestamp and make checkpoint if timestamp is large enough.
      * @returns error code and flag that indicates whether or not new checkpoint is created
      */
    int check_timestamp_(aku_TimeStamp ts, Lock &lock);

    void filter(const SortedRun *run, SearchQuery const& q, std::vector<PSortedRun> *results) const;
};
}
