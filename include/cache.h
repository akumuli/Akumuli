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

#include <stx/btree_multimap.h>

#include <tuple>
#include <vector>
#include <memory>

namespace Akumuli {

struct Generation {
    //! TTL
    TimeDuration ttl_;

    //! Container type
    typedef stx::btree_multimap<std::tuple<TimeStamp, ParamId>, EntryOffset> MapType;

    //! Index
    std::unique_ptr<MapType> data_;

    //! Normal c-tor
    Generation(TimeDuration ttl) noexcept;

    //! Copy c-tor
    Generation(Generation const& other) noexcept;

    //! Move c-tor
    Generation(Generation && other) noexcept;

    //! Add item to cache
    void add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept;

    //! Search
    std::vector<EntryOffset> get(TimeStamp ts, ParamId pid) noexcept;

    /** Get the oldest timestamp of the generation.
     *  If generation is empty - return false, true otherwise.
     */
    bool get_oldest_timestamp(TimeStamp* ts) const noexcept;
};


class Cache {
    PageHeader* page_;
    TimeDuration ttl_;
    // Index structures
    std::vector<Generation> gen_;
public:
    Cache(TimeDuration ttl, PageHeader* page);

    void add_entry(const Entry& entry, EntryOffset offset) noexcept;

    void add_entry(const Entry2& entry, EntryOffset offset) noexcept;
};

}
