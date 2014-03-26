/**
 * PRIVATE HEADER
 *
 * Counting is HARD!
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 */

#pragma once
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <boost/smart_ptr/detail/spinlock.hpp>
#include <tbb/enumerable_thread_specific.h>

#include "util.h"

namespace Akumuli {

typedef boost::detail::spinlock SpinLock;

/** Simple concurrent limit counter.
 *  Avoids contention but stil uses synchronization in fast path.
 */
struct LimitCounter {

    static const int64_t MAX_RESERVE = 0x1000;
    static const int64_t THRESHOLD = 0x10;

    struct CounterWithPad {
        int64_t value_;
        int64_t limit_;
        char padding_[128 - 2*sizeof(int64_t)];

        CounterWithPad()
            : value_(0)
            , limit_(0)
        {}
    };

    typedef tbb::enumerable_thread_specific<CounterWithPad> ThreadLocalCounter;

    ThreadLocalCounter          counters_;      //! Per-thread counters
    const int64_t               total_limit_;   //! Counter limit
    SpinLock                    counter_lock_;  //! Object lock
    int64_t                     reserved_;      //! Reserved value
    int64_t                     counted_;       //! Number of decrements

    //! C-tor
    LimitCounter(int64_t max_value)
        : total_limit_(max_value)
        , reserved_(0)
        , counted_(0)
        , counter_lock_()
    {
        if(max_value < THRESHOLD) {  // Panic!
            throw std::runtime_error("Cache size limit is to small");
        }
    }

    //! Calculate precise balance (number of decrements)
    size_t precise() const noexcept {
        size_t total = counted_;
        for(auto i = counters_.begin(); i != counters_.end(); i++) {
            total += i->limit_ - i->value_;
        }
        return total;
    }

    //! Decrement limit counter
    bool dec() noexcept {
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
        balance /= counters_.size();
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
};

}
