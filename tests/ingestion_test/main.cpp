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

const int DB_SIZE = 4;
const int NUM_ITERATIONS = 10*1000*1000;

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
    aku_ParamId params[] = {1};
    aku_SelectQuery* query = aku_make_select_query( std::numeric_limits<aku_TimeStamp>::min()
                                                  , std::numeric_limits<aku_TimeStamp>::max()
                                                  , 1, params);
    aku_Cursor* cursor = aku_select(db, query);
    aku_TimeStamp current_time = 0;
    while(!aku_cursor_is_done(cursor)) {
        int err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            std::cout << aku_error_message(err) << std::endl;
            return -1;
        }
        aku_Entry const* entries[1000];
        int n_entries = aku_cursor_read(cursor, entries, 1000);
        for (int i = 0; i < n_entries; i++) {
            aku_Entry const* p = entries[i];
            if (p->time != current_time) {
                std::cout << "Error at " << current_time << std::endl;
                return -2;
            }
            current_time++;
        }
    }
    delete_storage();
    return 0;
}
