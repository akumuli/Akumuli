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
#include <tbb/spin_mutex.h>
#include <tbb/enumerable_thread_specific.h>

#include "util.h"

namespace Akumuli {

typedef tbb::spin_mutex SpinLock;

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
    LimitCounter(int64_t max_value);

    //! Calculate precise balance (number of decrements)
    size_t precise() const noexcept;

    //! Decrement limit counter
    bool dec() noexcept;
};

}
