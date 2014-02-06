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
#include <deque>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

namespace Akumuli {

namespace details {

    //! Intrusive list tag for generation class (incomplete type)
    struct gen_list_tag_;

    //! Intrusive set tag for generation class (incomplete type)
    struct gen_set_tag_;

    typedef boost::intrusive::list_base_hook<boost::intrusive::tag<gen_list_tag_> > GenerationListBaseHook;

    typedef boost::intrusive::set_base_hook<boost::intrusive::tag<gen_set_tag_> > GenerationSetBaseHook;

}

struct Generation : details::GenerationListBaseHook
{
    // TODO: add fine grained locking
    //! Container type
    typedef btree::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;

    size_t                  capacity_;     //< Max generation size
    MapType                 data_;         //< Dictionary

    // Public properties

    TimeStamp               baseline;
    int                     state;

    //! Normal c-tor
    Generation(size_t max_size) noexcept;

    //! Copy c-tor
    Generation(Generation const& other);

    void swap(Generation& other);

    /**  Add item to cache.
      *  @return AKU_WRITE_STATUS_OVERFLOW if generation is full. Note that write is successful anyway.
      */
    int add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    /** Search for range of elements.
      */
    void search(SingleParameterCursor* cursor) const noexcept;

    //! Get number of items
    size_t size() const noexcept;

    MapType::const_iterator begin() const;

    MapType::const_iterator end() const;
};


// Cache list type
typedef boost::intrusive::list< Generation
                              , boost::intrusive::base_hook<details::GenerationListBaseHook>
                              , boost::intrusive::constant_time_size<false>
                              , boost::intrusive::link_mode<boost::intrusive::normal_link> >
GenListType;


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
    int                     shift_;         //< Shift width
    TimeStamp               baseline_;      //< Cache baseline
    std::deque<Generation>  cache_;         //< List of all generations
    GenListType             free_list_;     //< List of available gen-s
    GenListType             gen_;           //< Active gen-s
    GenListType             swap_;          //< Swap

    /* NOTE:
     * Generation must be isoated (doesn't interleave with each other).
     *
     * [Gen0)[Gen1)[Gen2) -> write to indirection vector
     *
     * Every generation must hold one half open time interval [tbegin, tend).
     * Client process can pop out generations from the end of the queue. This
     * requirement needed to implement search and pop-out procedures more efficiently,
     * without merging.
     */

    int add_entry_(TimeStamp ts, ParamId pid, EntryOffset offset, size_t* nswapped) noexcept;

    void swapn(int swaps) noexcept;

    /** Allocate `ngens` generations from free_list_
      * and put them before the first element of the
      * gen_ list.
      */
    void allocate_from_free_list(int ngens) noexcept;
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
    void search(SingleParameterCursor* cursor) const noexcept;

    //! Remove all data
    void clear() noexcept;
};

}
