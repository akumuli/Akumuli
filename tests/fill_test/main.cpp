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
#include <apr_mmap.h>
#include <apr_general.h>

#include "akumuli.h"
#include "page.h"
#include "storage.h"

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
