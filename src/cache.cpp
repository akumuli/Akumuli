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

Generation::Generation(TimeDuration ttl, size_t max_size, uint32_t starting_index) noexcept
    : ttl_(ttl)
    , max_size_(max_size)
    , starting_index_(starting_index)
{
}

Generation::Generation(Generation && other) noexcept
    : ttl_(other.ttl_)
    , max_size_(other.max_size_)
    , starting_index_(other.starting_index_)
{
    other.data_.swap(data_);
}

bool Generation::get_oldest_timestamp(TimeStamp* ts) const noexcept {
    if (data_.empty())
        return false;
    auto iter = data_.begin();
    *ts = std::get<0>(iter->first);
    return true;
}

void Generation::add(TimeStamp ts, ParamId param, EntryOffset  offset) noexcept {
    auto key = std::make_tuple(ts, param);
    data_.insert(std::make_pair(key, offset));
}

std::pair<size_t, bool> Generation::find(TimeStamp ts, ParamId pid, EntryOffset* results, size_t results_len, size_t skip) noexcept {
    auto key = std::make_tuple(ts, pid);
    auto iter_pair = data_.equal_range(key);
    size_t result_ix = 0;
    for (;iter_pair.first != iter_pair.second; iter_pair.first++) {
        if (skip) {
            skip--;
        } else {
            if (result_ix == results_len) break;
            results[result_ix++] = iter_pair.first->second;
        }
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
