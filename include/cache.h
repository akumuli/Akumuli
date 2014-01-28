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

#include <cpp-btree/btree_map.h>

#include <tuple>
#include <vector>
#include <memory>

namespace Akumuli {

struct Generation {
    //! Container type
    typedef btree::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;

    TimeDuration ttl_;          //< TTL
    size_t       capacity_;     //< Max generation size
    TimeStamp    most_recent_;  //< Most recent timestamp
    MapType      data_;         //< Dictionary

    //! Normal c-tor
    Generation(TimeDuration ttl, size_t max_size) noexcept;

    //! Copy c-tor
    Generation(Generation const& other);

    void swap(Generation& other);

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if generation is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for elements.
     *  @param ts time stamp
     *  @param pid parameter id
     *  @param results destination array
     *  @param results_len size of the destination array
     *  @param skip number of elements to skip
     *  @return number of returned elements x is there any elements remaining
     */
    std::pair<size_t, bool> find(TimeStamp ts, ParamId pid, EntryOffset* results, size_t results_len, size_t skip) noexcept;

    /** Get the oldest timestamp of the generation.
     *  If generation is empty - return false, true otherwise.
     */
    bool get_oldest_timestamp(TimeStamp* ts) const noexcept;

    /** Get the most recent timestamp if it present.
      * If generation is empty - return false.
      */
    bool get_most_recent_timestamp(TimeStamp* ts) const noexcept;

    //! Get number of items
    size_t size() const noexcept;

    //! Close generation for write
    void close();

    MapType::const_iterator begin() const;

    MapType::const_iterator end() const;
};


/** Cache for the time-series data.
  * @note This is a first _sketch_ implementation. It's not as good as it can be
  *       but it is good enough for the first try.
  * Time series data is stored b-tree. If tree is full or out of date (there is limit
  * on tree size and elements age) - new tree is created and the old one can be writen
  * back to the page. The individual trees is implemented by the "Generation" class.
  * "Cache" class is actually a list of generations and public interface.
  */
class Cache {
    TimeDuration            ttl_;           //< TTL
    size_t                  max_size_;      //< Max size of the generation
    std::vector<Generation> gen_;           //< List of generations

    int add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset) noexcept;
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
    int add_entry(const Entry& entry, EntryOffset offset) noexcept;

    /** Add entry to cache.
     *  @return write status. If status is AKU_WRITE_STATUS_OVERFLOW - cache eviction must be performed.
     */
    int add_entry(const Entry2& entry, EntryOffset offset) noexcept;

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
    void search(SingleParameterCursor* cursor) const noexcept;

    //! Remove all data
    void clear() noexcept;

    /** Checks whether time stamp is to late.
      * @param ts time stamp to check
      * @return true if time stamp is late
      * @note late write depth can be dynamically adjusted if cache uses too much
      * memory - late write depth shrinks.
      */
    bool is_too_late(TimeStamp ts) noexcept;
};

}
