#include <iostream>
#include <cassert>
#include <random>
#include <tuple>
#include <map>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <memory>

#include <boost/unordered_map.hpp>
#include <cpp-btree/btree_map.h>

#include <boost/timer.hpp>
#include <boost/pool/pool.hpp>
#include <boost/pool/pool_alloc.hpp>
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

const int DB_SIZE = 4;
const int NUM_ITERATIONS = 100*1000*1000;

const char* DB_NAME = "test";
const char* DB_PATH = "./test";
const char* DB_META_FILE = "./test/test.akumuli";

void delete_storage() {
    boost::filesystem::remove_all(DB_PATH);
}

int main(int cnt, const char** args)
{
    int steps = 2; //boost::lexical_cast<int>(args[1]);
    if (steps--)
    {
        std::cout << "Sequencer perf-test, ordered timestamps" << std::endl;
        // Patience sort perf-test
        boost::timer timer;
        size_t ix_merged = 0;
        Sequencer seq(nullptr, {10000});
        for (int ix = 0u; ix < NUM_ITERATIONS; ix++) {
            TimeSeriesValue value({(int64_t)ix}, ix & 0xFF, (EntryOffset)ix);
            int status;
            Sequencer::Lock lock;
            tie(status, lock) = seq.add(value);
            if (lock.owns_lock()) {
                CursorResult results[0x10000];
                BufferedCursor cursor(results, 0x10000);
                Caller caller;
                seq.merge(caller, &cursor, std::move(lock));
                for (size_t i = 0; i < cursor.count; i++) {
                    if (cursor.offsets_buffer[i].first != ix_merged) {
                        // report error
                        std::cout << "Error at: " << i << " " << cursor.offsets_buffer[i] << " != " << ix_merged << std::endl;
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
    if (steps--)
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
                    TimeSeriesValue value({(int64_t)ixx}, ixx & 0xFF, (EntryOffset)ixx);
                    int status;
                    Sequencer::Lock lock;
                    tie(status, lock) = seq.add(value);
                    if (lock.owns_lock()) {
                        CursorResult results[0x10000];
                        BufferedCursor cursor(results, 0x10000);
                        Caller caller;
                        seq.merge(caller, &cursor, std::move(lock));
                        for (size_t i = 0; i < cursor.count; i++) {
                            if (cursor.offsets_buffer[i].first != ix_merged) {
                                // report error
                                std::cout << "Error at: " << i << " " << cursor.offsets_buffer[i] << " != " << ix_merged << std::endl;
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
    /* Delete old
     * Create database
     * Fill
     * Close
     * Read and check
     */

    // Cleanup
    delete_storage();

    // Create database
    apr_status_t result = create_database(DB_NAME, DB_PATH, DB_PATH, DB_SIZE);
    if (result != APR_SUCCESS) {
        std::cout << "Error in new_storage" << std::endl;
        return (int)result;
    }

    aku_Config config;
    config.debug_mode = 0;
    config.max_cache_size = 10*1000*1000;
    config.max_late_write = 10000;
    auto db = aku_open_database(DB_META_FILE, config);

    boost::timer timer;
    for(int64_t i = 0; i < NUM_ITERATIONS; i++) {
        apr_time_t now = i;
        aku_MemRange memr;
        memr.address = (void*)&i;
        memr.length = sizeof(i);
        aku_add_sample(db, 1, now, memr);
        if (i % 1000000 == 0) {
            std::cout << i << " " << timer.elapsed() << "s" << std::endl;
            timer.restart();
        }
    }
    aku_close_database(db);

    // Search

    delete_storage();
    return 0;
}
