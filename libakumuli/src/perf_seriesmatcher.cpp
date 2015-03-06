#include <iostream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <time.h>
#include <stdio.h>

#include "util.h"
#include "seriesparser.h"

using namespace Akumuli;

const int NELEMENTS = 1000000;

class PerfTimer
{
public:
    PerfTimer();
    void   restart();
    double elapsed() const;
private:
    timespec _start_time;
};

PerfTimer::PerfTimer() {
    clock_gettime(CLOCK_MONOTONIC_RAW, &_start_time);
}

void PerfTimer::restart() {
    clock_gettime(CLOCK_MONOTONIC_RAW, &_start_time);
}

double PerfTimer::elapsed() const {
    timespec curr;
    clock_gettime(CLOCK_MONOTONIC_RAW, &curr);
    return double(curr.tv_sec - _start_time.tv_sec) +
           double(curr.tv_nsec - _start_time.tv_nsec)/1000000000.0;
}

int main() {
    SeriesMatcher matcher(1ul);

    PerfTimer tm;
    const char *series_name_fmt = "memory host=%d port=%d";
    // Load data to the matcher
    char input[0x1000];
    char output[0x1000];
    for(int i = 0; i < NELEMENTS; i++) {
        int n = sprintf(input, series_name_fmt, i%100000, i%100000);
        const char* keystr = nullptr;
        const char* outend = nullptr;
        SeriesParser::to_normal_form(input, input+n, output, output+n+1, &keystr, &outend);
        matcher.add(output, outend);
    }
    double elapsed = tm.elapsed();
    std::cout << "Putting " << NELEMENTS << " values to the matcher in "
              << elapsed << " seconds" << std::endl;
}

