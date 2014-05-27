#pragma once
//#include <time.h>
//#include <stdlib.h>
//#include <stdio.h>
//#include <stdint.h>

// TODO: use conditional compilation
// NOTE: x64 only!
static __inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

/** Simple TSC based timer.
  * @code CPUTimer t;
  *       t.resume();  // start counting
  *       ...
  *       t.pause();  // done counting
  *       t.elapsed();  // get elapsed time in CPU cycles
  *       t.reset();  // reuse timer
  */
class CPUCounter {
    unsigned long long begin_;
    unsigned long long elapsed_;
    //timespec begin;
public:
    CPUCounter()
        : begin_(0)
        , elapsed_(0)
    {
    }

    void pause() noexcept {
        elapsed_ += rdtsc() - begin_;
    }

    void resume() noexcept {
        begin_ = rdtsc();
    }

    unsigned long long elapsed() const noexcept {
        return elapsed_;
    }

    void reset() noexcept {
        elapsed_ = 0;
        begin_ = rdtsc();
    }
};
