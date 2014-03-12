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

// needed for timers
#include <boost/asio.hpp>

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
        counter.value_++;
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

struct LimitCounter {
};

}
