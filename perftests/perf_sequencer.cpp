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

const u32 NUM_ITERATIONS = 100*1000*1000;


//! Simple static buffer cursor
struct BufferedCursor : InternalCursor {
    aku_Sample* results_buffer;
    size_t buffer_size;
    size_t count;
    bool completed = false;
    aku_Status error_code = AKU_SUCCESS;
    //! C-tor
    BufferedCursor(aku_Sample* buf, size_t size)
        : results_buffer(buf)
        , buffer_size(size)
        , count(0)
    {
    }

    bool put(Caller&, aku_Sample const& result) {
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

    void set_error(Caller&, aku_Status code) {
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
        aku_FineTuneParams params = {};
        params.window_size = 10000;
        Sequencer seq(nullptr, params);
        for (u32 ix = 0u; ix < NUM_ITERATIONS; ix++) {
            TimeSeriesValue value({(u64)ix}, ix & 0xFF, (double)ix);
            int status = 0;
            int lock = 0;
            tie(status, lock) = seq.add(value);
            if (lock % 2 == 1) {
                aku_Sample results[0x10000];
                BufferedCursor cursor(results, 0x10000);
                Caller caller;
                seq.merge(caller, &cursor);
                for (size_t i = 0; i < cursor.count; i++) {
                    size_t actual = static_cast<size_t>(cursor.results_buffer[i].timestamp);
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
        u32 buffer[buffer_size];
        int buffer_ix = buffer_size;
        Sequencer seq(nullptr, {10000});
        for (u32 ix = 0u; ix < NUM_ITERATIONS; ix++) {
            buffer_ix--;
            buffer[buffer_ix] = ix;
            if (buffer_ix == 0) {
                buffer_ix = buffer_size;
                for(auto ixx: buffer) {
                    TimeSeriesValue value({(u64)ixx}, ixx & 0xFF, (double)ixx);
                    int status = 0;
                    int lock = 0;
                    tie(status, lock) = seq.add(value);
                    if (lock % 2 == 1) {
                        aku_Sample results[0x10000];
                        BufferedCursor cursor(results, 0x10000);
                        Caller caller;
                        seq.merge(caller, &cursor);
                        for (size_t i = 0; i < cursor.count; i++) {
                            auto offset = static_cast<size_t>(cursor.results_buffer[i].timestamp);
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
