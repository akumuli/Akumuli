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

#include <cpp-btree/btree_map.h>

#include <tuple>
#include <vector>
#include <deque>
#include <memory>
#include <mutex>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

namespace Akumuli {

/*
    Write -> Bucket -> round robin -> Sequence
    Search -> Bucket -> map search Sequence -> merge results
*/


namespace details {

    //! Intrusive list tag for bucket class (incomplete type)
    struct bucket_list_tag_;

    //! Intrusive set tag for bucket class (incomplete type)
    struct bucket_set_tag_;

    typedef boost::intrusive::list_base_hook<boost::intrusive::tag<bucket_list_tag_> > BucketListBaseHook;

    typedef boost::intrusive::set_base_hook<boost::intrusive::tag<bucket_set_tag_> > BucketSetBaseHook;

}

struct Sequence
{
    // TODO: add fine grained locking
    //! Container type
    typedef btree::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;
    typedef std::tuple<TimeStamp, ParamId, EntryOffset> ValueType;

    size_t                  capacity_;      //< Max seq size
    MapType                 data_;          //< Dictionary
    mutable std::mutex      obj_mtx_;       //< data_ mutex
    mutable std::mutex      tmp_mtx_;       //< temp_ mutex
    std::vector<ValueType>  temp_;          //< Temporary storage

    //! Normal c-tor
    Sequence(size_t max_size) noexcept;

    //! Copy c-tor
    Sequence(Sequence const& other);

    Sequence& operator = (Sequence const& other);

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if sequence is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(Caller& caller, InternalCursor* cursor, SingleParameterSearchQuery const& query) const noexcept;

    //! Get number of items
    size_t size() const noexcept;

    MapType::const_iterator begin() const;

    MapType::const_iterator end() const;
};


//! Bucket of N sequnces.
struct Bucket : details::BucketListBaseHook {

    std::vector<Sequence>   seq_;
    std::atomic<int>        rrindex_;  //< round robin index (maybe I can use TSC register instead of this)
    int64_t                 baseline;  //< max timestamp for the bucket
    std::atomic<int>        state;     //< state of the bucket (0 - active, 1 - ready)

    /** C-tor
      * @param n number of sequences
      * @param max_size max size of the sequence
      */
    Bucket(int n, size_t max_size, int64_t baseline);

    //! Copy c-tor
    Bucket(Bucket const& other);

    Bucket& operator = (Bucket const& other);

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if bucket sequence is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(SingleParameterSearchQuery* params, InternalCursor* cursor) const noexcept;

    /** Merge all offsets in one list in order.
      * @param cur read cursor
      * @param page this bucket owner
      * @return AKU_EBUSY if bucket is not ready AKU_SUCCESS otherwise
      */
    int merge(Caller& caller, InternalCursor* cur, PageHeader* page) const noexcept;
};


// Cache list type
typedef boost::intrusive::list< Bucket
                              , boost::intrusive::base_hook<details::BucketListBaseHook>
                              , boost::intrusive::constant_time_size<false>
                              , boost::intrusive::link_mode<boost::intrusive::normal_link> >
BucketListType;


/** Cache for the time-series data.
  * @note This is a first _sketch_ implementation. It's not as good as it can be
  *       but it is good enough for the first try.
  * Time series data is stored b-tree. If tree is full or out of date (there is limit
  * on tree size and elements age) - new tree is created and the old one can be writen
  * back to the page. The individual trees is implemented by the "Sequence" class.
  * "Cache" class is actually a list of buckets and public interface.
  */
class Cache {
    TimeDuration            ttl_;           //< TTL
    size_t                  max_size_;      //< Max size of the sequence
    int                     bucket_size_;   //< Bucket size
    int                     shift_;         //< Shift width
    int64_t                 baseline_;      //< Cache baseline
    std::deque<Bucket>      buckets_;       //< List of all buckets
    BucketListType          free_list_;     //< List of available buckets
    BucketListType          cache_;         //< Active cache
    mutable std::mutex      lists_mutex_;   //< Mutex that guards both lists, baseline and deque

    /* NOTE:
     * Buckets must be isoated (doesn't interleave with each other).
     *
     * [Bucket0)[Bucket1)[Bucket2) -> writes to indirection vector
     *
     * Every bucket must hold one half open time interval [tbegin, tend).
     * Client process can pop out buckets from the end of the queue. This
     * requirement needed to implement search and pop-out procedures more efficiently,
     * without merging.
     */

    int add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept;

    /** Allocate n Buckets from free_list_
      * and put them before the first element of the
      * cache_ list.
      */
    void allocate_from_free_list(int n, int64_t last_baseline) noexcept;
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
     *  @param offsets ret-value, array of offsets ordered by timestamp and paramId
     *  @param size offsets size
     *  @param noffsets number of returned elements
     *  @return operation status AKU_SUCCESS on success - error code otherwise (AKU_ENO_MEM or AKU_ENO_DATA)
     */
    int remove_old(EntryOffset* offsets, size_t size, uint32_t* noffsets) noexcept;

    // TODO: split remove_old f-n into two parts, one that removes old data from cache and one that reads data

    /** Search fun-n that is similar to Page::search
      */
    void search(SingleParameterSearchQuery* cursor) const noexcept;

    //! Remove all data
    void clear() noexcept;
};

}
