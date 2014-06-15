/**
 * PRIVATE HEADER
 *
 * Data structures for main memory storage.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */


#pragma once
#include "page.h"
#include "cursor.h"
#include "counters.h"

#include <cpp-btree/btree_map.h>

#include <tuple>
#include <vector>
#include <algorithm>
#include <deque>
#include <memory>
#include <mutex>

#include <tbb/enumerable_thread_specific.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

namespace Akumuli {

struct TimeSeriesValue {
    std::tuple<TimeStamp, ParamId> key_;
    EntryOffset value;

    TimeSeriesValue();

    TimeSeriesValue(TimeStamp ts, ParamId id, EntryOffset offset);

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

    std::vector<SortedRun>       runs_;           //< Active sorted runs
    std::vector<SortedRun>       ready_;          //< Ready to merge
    SortedRun                    key_;
    const TimeDuration           window_size_;
    const PageHeader* const      page_;
    TimeStamp                    top_timestamp_;  //< Largest timestamp ever seen
    uint32_t                     checkpoint_;     //< Last checkpoint timestamp
    mutable std::mutex           progress_flag_;
    std::vector<std::atomic_flag> run_lock_flags_;

    Sequencer(PageHeader const* page, TimeDuration window_size);

    //! Checkpoint id = ⌊timestamp/window_size⌋
    uint32_t get_checkpoint_(TimeStamp ts) const noexcept;

    //! Convert checkpoint id to timestamp
    TimeStamp get_timestamp_(uint32_t cp) const noexcept;

    // move sorted runs to ready_ collection
    bool make_checkpoint_(uint32_t new_checkpoint) noexcept;

    /** Check timestamp and make checkpoint if timestamp is large enough.
      * @returns error code and flag that indicates whether or not new checkpoint is created
      */
    std::tuple<int, bool> check_timestamp_(TimeStamp ts) noexcept;

    /** Add new sample to sequence.
      * @brief Timestamp of the sample can be out of order.
      * @returns error code and flag that indicates whether of not new checkpoint is createf
      */
    std::tuple<int, bool> add(TimeSeriesValue const& value);

    bool close();

    void merge(Caller& caller, InternalCursor* out_iter) noexcept;

    void filter(SortedRun& run, SearchQuery const& q, std::vector<SortedRun> results) const noexcept;

    void lock_run(int ix) const noexcept;
    void unlock_run(int ix) const noexcept;

    // Searching

    void search(Caller& caller, InternalCursor* cur, SeqrchQuery const& query) const noexcept;
};


// Deprecated

struct Sequence
{
    //! Container type
    typedef std::tuple<TimeStamp, ParamId> KeyType;
    typedef btree::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;
    typedef std::tuple<TimeStamp, ParamId, EntryOffset> ValueType;

    MapType                 data_;          //< Dictionary
    mutable std::mutex      obj_mtx_;       //< data_ mutex
    mutable std::mutex      tmp_mtx_;       //< temp_ mutex
    std::vector<ValueType>  temp_;          //< Temporary storage

    Sequence() {}

    Sequence(Sequence const& other) = delete;
    Sequence& operator = (Sequence const& other) = delete;

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if sequence is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(Caller& caller, InternalCursor* cursor, SearchQuery const& query, PageHeader* page) const noexcept;

    //! Get number of items
    size_t size() const noexcept;

    MapType::const_iterator begin() const;

    MapType::const_iterator end() const;

    void get_all(Caller& caller, InternalCursor* cursor, PageHeader* page) const noexcept;
};


//! Bucket of N sequnces.
struct Bucket {

    typedef tbb::enumerable_thread_specific<Sequence> SeqList;
    SeqList seq_;
    LimitCounter limit_;
    const int64_t baseline;  //< max timestamp for the bucket
    std::atomic<int> state;

    /** C-tor
      * @param size_limit max size of the bucket
      * @param baseline baseline timestamp value
      */
    Bucket(int64_t size_limit, int64_t baseline);

    Bucket(Bucket const& other) = delete;
    Bucket& operator = (Bucket const& other) = delete;

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if bucket sequence is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(Caller& caller, InternalCursor* cursor, const SearchQuery &params, PageHeader* page) const noexcept;

    /** Merge all offsets in one list in order.
      * @param cur read cursor
      * @param page this bucket owner
      * @return AKU_EBUSY if bucket is not ready AKU_SUCCESS otherwise
      */
    int merge(Caller& caller, InternalCursor* cur, PageHeader *page) const noexcept;

    size_t precise_count() const noexcept;
};


/** Cache for the time-series data.
  * @note This is a first _sketch_ implementation. It's not as good as it can be
  *       but it is good enough for the first try.
  * Time series data is stored b-tree. If tree is full or out of date (there is limit
  * on tree size and elements age) - new tree is created and the old one can be writen
  * back to the page. The individual trees is implemented by the "Sequence" class.
  * "Cache" class is actually a list of buckets and public interface.
  */
class Cache {
    typedef tbb::tbb_allocator<Bucket>                  BucketAllocator;
    typedef tbb::concurrent_hash_map<int64_t, Bucket*>  TableType;
    typedef std::deque<Bucket*>                         BucketsList;
    typedef std::mutex                                  LockType;
    typedef std::pair<int64_t, int64_t>                 BaselineBounds;
    // ---------
    int64_t                 baseline_;      //< Cache baseline
    TableType               cache_;         //< Active cache
    BucketsList             ordered_buckets_;  //< Live objects
    mutable LockType        lock_;
    TimeDuration            ttl_;           //< TTL
    size_t                  max_size_;      //< Max size of the sequence
    int                     shift_;         //< Shift width
    BucketAllocator         allocator_;     //< Concurrent bucket allocator
    BaselineBounds          minmax_;        //< Min and max baselines
    PageHeader*             page_;          //< Page header for searching


    /* NOTE:
     * Buckets must be isoated (doesn't interleave with each other).
     *
     * [Bucket0)[Bucket1)[Bucket2) -> writes to indirection vector
     */

    int add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept;

    void update_minmax_() noexcept;

public:
    /** C-tor
      * @param ttl max late write timeout
      * @param max_size max number of elements to hold
      */
    Cache( TimeDuration     ttl
         , size_t           max_size
         , PageHeader*      page);

    /** Add entry to cache.
     *  @return write status. If status is AKU_WRITE_STATUS_OVERFLOW - cache eviction must be performed.
     */
    int add_entry(const Entry& entry, EntryOffset offset, size_t* nswapped) noexcept;

    /** Add entry to cache.
     *  @return write status. If status is AKU_WRITE_STATUS_OVERFLOW - cache eviction must be performed.
     */
    int add_entry(const Entry2& entry, EntryOffset offset, size_t* nswapped) noexcept;

    /** Remove oldest elements from cache and return them to caller.
     *  Out buffer must be large enough to store all entries from one bucket.
     *  @param offsets ret-value, array of offsets ordered by timestamp and paramId
     *  @param size offsets size
     *  @param noffsets number of returned elements
     *  @param page pointer to buckets page
     *  @return operation status AKU_SUCCESS on success - error code otherwise (AKU_ENO_MEM or AKU_ENO_DATA)
     */
    int pick_last(CursorResult* offsets, size_t size, size_t* noffsets) noexcept;

    /** Search fun-n that is similar to Page::search
      */
    void search(Caller &caller, InternalCursor *cur, SearchQuery &query) const noexcept;

    //! Remove all data
    void clear() noexcept;
};

}
