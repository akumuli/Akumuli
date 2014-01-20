/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */

#define AKU_NUM_GENERATIONS 5

#include "cache.h"

namespace Akumuli {

// Generation ---------------------------------

Generation::Generation(TimeDuration ttl) noexcept
    : ttl_(ttl)
{
    data_.reset(new MapType());
}

Generation::Generation(Generation const& other) noexcept
    : ttl_(other.ttl_)
{
    data_.reset(new MapType(*other.data_));
}

Generation::Generation(Generation && other) noexcept
    : ttl_(other.ttl_)
    , data_(std::move(other.data_))
{
}

bool Generation::get_oldest_timestamp(TimeStamp* ts) const noexcept {
    return false;
}

void Generation::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    auto key = std::make_tuple(ts, param);
    data_->insert(std::make_pair(key, offset));
}

std::pair<size_t, bool> Generation::find(TimeStamp ts, ParamId pid, EntryOffset* results, size_t results_len, size_t skip) noexcept {
    auto key = std::make_tuple(ts, pid);
    auto iter_pair = data_->equal_range(key);
    size_t result_ix = 0;
    for (; iter_pair.first != iter_pair.second && result_ix < results_len; iter_pair.first++) {
        if (result_ix >= skip)
            results[result_ix++] = iter_pair.first->second;
    }
    return std::make_pair(result_ix, iter_pair.first != iter_pair.second);
}



// Cache --------------------------------------

Cache::Cache(TimeDuration ttl, PageHeader* page)
    : ttl_(ttl)
    , page_(page)
{
    //for(int i = 0; i < AKU_NUM_GENERATIONS; i++)
    //    gen_.push_back(Generation(ttl_));
}

void Cache::add_entry(const Entry& entry, EntryOffset offset) noexcept {
    //gen_[0].add(entry.time, entry.param_id, offset);
}


}
