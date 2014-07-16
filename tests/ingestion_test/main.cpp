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
const int CHUNK_SIZE = 10*1000;

const char* DB_NAME = "test";
const char* DB_PATH = "./test";
const char* DB_META_FILE = "./test/test.akumuli";

void delete_storage() {
    boost::filesystem::remove_all(DB_PATH);
}

void query_database(aku_Database* db, aku_TimeStamp begin, aku_TimeStamp end) {
    boost::timer timer;
    aku_ParamId params[] = {1};
    aku_SelectQuery* query = aku_make_select_query( begin
                                                  , end
                                                  , 1, params);
    timer.restart();
    aku_Cursor* cursor = aku_select(db, query);
    aku_TimeStamp current_time = begin;
    while(!aku_cursor_is_done(cursor)) {
        int err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            std::cout << aku_error_message(err) << std::endl;
            return;
        }
        aku_Entry const* entries[1000];
        int n_entries = aku_cursor_read(cursor, entries, 1000);
        for (int i = 0; i < n_entries; i++) {
            aku_Entry const* p = entries[i];
            if (p->time != current_time) {
                std::cout << "Error at " << current_time << " expected " << current_time << " acutal " << p->time  << std::endl;
                return;
            }
            current_time++;
        }
        if (current_time % 1000000 == 0) {
            std::cout << current_time << " " << timer.elapsed() << "s" << std::endl;
            timer.restart();
        }
    }
    aku_close_cursor(cursor);
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
    for(uint64_t i = 0; i < NUM_ITERATIONS; i++) {
        aku_MemRange memr;
        memr.address = (void*)&i;
        memr.length = sizeof(i);
        aku_add_sample(db, 1, i, memr);
        if (i % 1000000 == 0) {
            std::cout << i << " " << timer.elapsed() << "s" << std::endl;
            timer.restart();
        }
    }

    // Search
    query_database( db
                  , std::numeric_limits<aku_TimeStamp>::min()
                  , std::numeric_limits<aku_TimeStamp>::max());

    // Random access
    std::vector<std::pair<aku_TimeStamp, aku_TimeStamp>> ranges;
    for (aku_TimeStamp i = 0u; i < (aku_TimeStamp)NUM_ITERATIONS/CHUNK_SIZE; i++) {
        ranges.push_back(std::make_pair((i - 1)*CHUNK_SIZE, i*CHUNK_SIZE));
    }

    std::random_shuffle(ranges.begin(), ranges.end());

    for(auto range: ranges) {
        query_database(db, range.first, range.second);
    }

    aku_close_database(db);
    delete_storage();
    return 0;
}
