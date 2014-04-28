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
#include <deque>
#include <memory>
#include <mutex>

#include <tbb/enumerable_thread_specific.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

namespace Akumuli {


struct Sequence
{
    //! Container type
    typedef btree::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;
    typedef std::tuple<TimeStamp, ParamId, EntryOffset> ValueType;

    MapType                 data_;          //< Dictionary
    mutable std::mutex      obj_mtx_;       //< data_ mutex
    mutable std::mutex      tmp_mtx_;       //< temp_ mutex
    std::vector<ValueType>  temp_;          //< Temporary storage

    //Sequence(Sequence const& other) = delete;
    //Sequence& operator = (Sequence const& other) = delete;

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if sequence is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(Caller& caller, InternalCursor* cursor, SearchQuery const& query) const noexcept;

    //! Get number of items
    size_t size() const noexcept;

    MapType::const_iterator begin() const;

    MapType::const_iterator end() const;
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
    void search(Caller& caller, InternalCursor* cursor, const SearchQuery &params) const noexcept;

    /** Merge all offsets in one list in order.
      * @param cur read cursor
      * @param page this bucket owner
      * @return AKU_EBUSY if bucket is not ready AKU_SUCCESS otherwise
      */
    int merge(Caller& caller, InternalCursor* cur, PageHeader* page) const noexcept;

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
    // ---------
    int64_t                 baseline_;      //< Cache baseline
    TableType               cache_;         //< Active cache
    BucketsList             live_buckets_;  //< Live objects
    mutable LockType        lock_;
    TimeDuration            ttl_;           //< TTL
    size_t                  max_size_;      //< Max size of the sequence
    int                     shift_;         //< Shift width
    BucketAllocator         allocator_;     //< Concurrent bucket allocator


    /* NOTE:
     * Buckets must be isoated (doesn't interleave with each other).
     *
     * [Bucket0)[Bucket1)[Bucket2) -> writes to indirection vector
     */

    int add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept;
public:
    /** C-tor
      * @param ttl max late write timeout
      * @param max_size max number of elements to hold
      */
    Cache( TimeDuration     ttl
         , size_t           max_size);

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
    int pick_last(EntryOffset* offsets, size_t size, size_t* noffsets, PageHeader *page) noexcept;

    // TODO: split remove_old f-n into two parts, one that removes old data from cache and one that reads data

    /** Search fun-n that is similar to Page::search
      */
    void search(Caller &caller, InternalCursor *cur, SearchQuery &query) const noexcept;

    //! Remove all data
    void clear() noexcept;
};

}
