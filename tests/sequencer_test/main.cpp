#include <iostream>
#include <cassert>
#include <random>
#include <tuple>
#include <map>
#include <vector>
#include <algorithm>
#include <memory>

#include <boost/timer.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <apr_mmap.h>
#include <apr_general.h>

#include "akumuli.h"
#include "page.h"
#include "storage.h"
#include "sequencer.h"

using namespace Akumuli;
using namespace std;

const int NUM_ITERATIONS = 100*1000*1000;


//! Simple static buffer cursor
struct BufferedCursor : InternalCursor {
    CursorResult* results_buffer;
    size_t buffer_size;
    size_t count;
    bool completed = false;
    int error_code = AKU_SUCCESS;
    //! C-tor
    BufferedCursor(CursorResult* buf, size_t size)
        : results_buffer(buf)
        , buffer_size(size)
        , count(0)
    {
    }

    bool put(Caller&, CursorResult const& result) {
        if (count == buffer_size) {
            completed = true;
            error_code = AKU_EOVERFLOW;
            return false;
        }
        results_buffer[count++] = result;
        return true;
    }

    void complete(Caller&) {
        completed = true;
    }

    void set_error(Caller&, int code) {
        completed = true;
        error_code = code;
    }
};


int main(int cnt, const char** args)
{
    aku_initialize(nullptr);
    {
        std::cout << "Sequencer perf-test, ordered timestamps" << std::endl;
        // Patience sort perf-test
        boost::timer timer;
        size_t ix_merged = 0;
        Sequencer seq(nullptr, {0, 10000, 0});
        for (int ix = 0u; ix < NUM_ITERATIONS; ix++) {
            TimeSeriesValue value({(uint64_t)ix}, ix & 0xFF, (aku_EntryOffset)ix, 8);
            int status = 0;
            int lock = 0;
            tie(status, lock) = seq.add(value);
            if (lock % 2 == 1) {
                CursorResult results[0x10000];
                BufferedCursor cursor(results, 0x10000);
                Caller caller;
                seq.merge(caller, &cursor);
                for (size_t i = 0; i < cursor.count; i++) {
                    size_t actual = reinterpret_cast<size_t>(cursor.results_buffer[i].data);
                    if (actual != ix_merged) {
                        // report error
                        std::cout << "Error at: " << i << " " << actual << " != " << ix_merged << std::endl;
                        return -1;
                    }
                    ix_merged++;
                }
            }
            if (ix % 1000000 == 0) {
                std::cout << ix << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
    }
    {
        std::cout << "Sequencer perf-test, unordered timestamps" << std::endl;
        // Patience sort perf-test
        boost::timer timer;
        size_t ix_merged = 0;
        const int buffer_size = 10000;
        int buffer[buffer_size];
        int buffer_ix = buffer_size;
        Sequencer seq(nullptr, {10000});
        for (int ix = 0u; ix < NUM_ITERATIONS; ix++) {
            buffer_ix--;
            buffer[buffer_ix] = ix;
            if (buffer_ix == 0) {
                buffer_ix = buffer_size;
                for(auto ixx: buffer) {
                    TimeSeriesValue value({(uint64_t)ixx}, ixx & 0xFF, (aku_EntryOffset)ixx, 8);
                    int status = 0;
                    int lock = 0;
                    tie(status, lock) = seq.add(value);
                    if (lock % 2 == 1) {
                        CursorResult results[0x10000];
                        BufferedCursor cursor(results, 0x10000);
                        Caller caller;
                        seq.merge(caller, &cursor);
                        for (size_t i = 0; i < cursor.count; i++) {
                            auto offset = reinterpret_cast<size_t>(cursor.results_buffer[i].data);
                            if (offset != ix_merged) {
                                // report error
                                std::cout << "Error at: " << i << " " << offset << " != " << ix_merged << std::endl;
                                return -1;
                            }
                            ix_merged++;
                        }
                    }
                }
            }
            if (ix % 1000000 == 0) {
                std::cout << ix << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
    }
    return 0;
}
