/*
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 */

#include "counters.h"
#include <mutex>

namespace Akumuli {

LimitCounter::LimitCounter(int64_t max_value)
    : total_limit_(max_value)
    , counter_lock_()
    , reserved_(0)
    , counted_(0)
{
    if(max_value < THRESHOLD) {  // Panic!
        throw std::runtime_error("Cache size limit is to small");
    }
}

size_t LimitCounter::precise() const noexcept {
    size_t total = counted_;
    for(auto i = counters_.begin(); i != counters_.end(); i++) {
        total += i->limit_ - i->value_;
    }
    return total;
}

bool LimitCounter::dec() noexcept {
    // fast path
    CounterWithPad& cnt = counters_.local();
    if (cnt.value_ > 0) {
        cnt.value_--;
        return true;
    }
    // slow path
    std::lock_guard<SpinLock> inner_guard(counter_lock_);
    reserved_ -= cnt.limit_;
    counted_ += cnt.limit_;
    // rebalance
    int64_t balance = (total_limit_ - (reserved_ + counted_));
    if (balance < THRESHOLD) {
        return false;
    }
    auto size = counters_.size();
    if (size == 0) size++;  // Size is zero sometimes. I don't know why, this shouldn't happen.
    balance /= size;
    if (balance > 0) {
        if (balance > MAX_RESERVE) {
            balance = MAX_RESERVE;
        }
        cnt.value_ = cnt.limit_ = balance;
        reserved_ += balance;
        return true;
    }
    return false;
}

}
