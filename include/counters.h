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

#include "util.h"

namespace Akumuli {

//! Eventually consistent counter
struct Counter {
    struct CounterWithPad {
        std::atomic<size_t> value_;
        char padding_[128 - sizeof(size_t)];
    };
    std::vector<CounterWithPad> counters_;
    std::atomic<size_t> value_;

    Counter(int period)
        : value_(0)
    {
        const int NUMCPU = std::thread::hardware_concurrency();
        counters_.resize(NUMCPU);
        for(auto& cnt: counters_) {
            cnt.value_.store(0);
        }
    }

    //! inconsistent increment
    void increment() {
        int ix = aku_getcpu();
        auto& counter = counters_[ix];
        counter.value_++;  // uncontended atomic increment
    }

    //! make counter value precise again
    void make_value() {
        std::atomic<int> a;
        for (CounterWithPad& cnt: counters_) {
            size_t old_value = cnt.value_.exchange(0);
            value_ += old_value;
        }
    }

    size_t get_value() const noexcept {
        return value_.load();
    }
};


typedef boost::detail::spinlock SpinLock;

/** Simple concurrent limit counter.
 *  Avoids contention but stil uses synchronization in fast path.
 */
struct LimitCounter {

    static const int64_t MAX_RESERVE = 0x1000;

    struct CounterWithPad {
        int64_t value_;
        int64_t limit_;
        SpinLock spinlock_;
        char padding_[128 - 2*sizeof(int64_t) - sizeof(spinlock_)];
    };

    std::vector<CounterWithPad> cvector_;       //! Per-CPU counters
    const int64_t               total_limit_;   //! Counter limit
    SpinLock                    counter_lock_;  //! Object lock
    int64_t                     reserved_;      //! Reserved value
    int64_t                     counted_;       //! Number of decrements

    //! C-tor
    LimitCounter(int64_t max_value)
        : total_limit_(max_value)
        , reserved_(0)
        , counted_(0)
    {
        const int NUMCPU = std::thread::hardware_concurrency();
        counters_.resize(NUMCPU);
        for(auto& cnt: counters_) {
            cnt.value_ = 0;
            cnt.limit_ = 0;
        }
    }

    //! Decrement limit counter
    bool dec() noexcept {
        // fast path
        CounterWithPad& cnt = counters_[aku_getcpu()];
        std::lock_guard<SpinLock> outer_guard(cnt.spinlock_);
        if (cnt.value_ > 0) {
            cnt.value_--;
            return true;
        }
        // slow path
        std::lock_guard<SpinLock> inner_guard(counter_lock);
        reserved_ -= cnt.limit_;
        counted_ += cnt.limit_;
        // rebalance
        int64_t balance = (total_limit_ - (reserved_ + counted_)) / NUMCPU;
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
