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

class CPUCounter {
    unsigned long long begin;
    //timespec begin;
public:
    CPUCounter() {
        reset();
    }

    unsigned long long elapsed() const noexcept {
        return rdtsc() - begin;
        //timespec end;
        //clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);
        //return end.tv_nsec - begin.tv_nsec;
    }

    void reset() noexcept {
        begin = rdtsc();
        //clock_gettime(CLOCK_THREAD_CPUTIME_ID, &begin);
    }
};
