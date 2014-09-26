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

const int DB_SIZE = 8;
const int NUM_ITERATIONS = 1000*1000*1000;
const int CHUNK_SIZE = 5000;

const char* DB_NAME = "test";
const char* DB_PATH = "./test";
const char* DB_META_FILE = "./test/test.akumuli";

void delete_storage() {
    boost::filesystem::remove_all(DB_PATH);
}

void print_storage_stats(aku_StorageStats& ss) {
    std::cout << ss.n_entries << " elements in" << std::endl
              << ss.n_volumes << " volumes with" << std::endl
              << ss.used_space << " bytes used and" << std::endl
              << ss.free_space << " bytes free" << std::endl;
}

void print_search_stats(aku_SearchStats& ss) {
    std::cout << "Interpolation search" << std::endl;
    std::cout << ss.istats.n_matches << " matches" << std::endl
              << ss.istats.n_times << " times" << std::endl
              << ss.istats.n_steps << " steps" << std::endl
              << ss.istats.n_overshoots << " overshoots" << std::endl
              << ss.istats.n_undershoots << " undershoots" << std::endl
              << ss.istats.n_reduced_to_one_page << "  reduced to page" << std::endl

              << ss.istats.n_page_in_core_checks << "  page_in_core checks" << std::endl
              << ss.istats.n_page_in_core_errors << "  page_in_core errors" << std::endl
              << ss.istats.n_pages_in_core_found  << "  page_in_core success" << std::endl
              << ss.istats.n_pages_in_core_miss  << "  page_in_core miss" << std::endl;

    std::cout << "Binary search" << std::endl;
    std::cout << ss.bstats.n_steps << " steps" << std::endl
              << ss.bstats.n_times << " times" << std::endl;

    std::cout << "Scan" << std::endl;
    std::cout << ss.scan.bwd_bytes << " bytes read in backward direction" << std::endl
              << ss.scan.fwd_bytes << " bytes read in forward direction" << std::endl;
}

aku_TimeStamp query_database(aku_Database* db, aku_TimeStamp begin, aku_TimeStamp end, uint64_t& counter, boost::timer& timer, uint64_t mod) {
    const int NUM_ELEMENTS = 1000;
    aku_ParamId params[] = {1};
    aku_SelectQuery* query = aku_make_select_query( begin
                                                  , end
                                                  , 1, params);
    aku_Cursor* cursor = aku_select(db, query);
    aku_TimeStamp current_time = begin;
    aku_TimeStamp last = begin;
    while(!aku_cursor_is_done(cursor)) {
        int err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            std::cout << aku_error_message(err) << std::endl;
            aku_close_cursor(cursor);
            return last;
        }
        aku_TimeStamp timestamps[NUM_ELEMENTS];
        aku_ParamId paramids[NUM_ELEMENTS];
        aku_PData pointers[NUM_ELEMENTS];
        uint32_t lengths[NUM_ELEMENTS];
        int n_entries = aku_cursor_read_columns(cursor, timestamps, paramids, pointers, lengths, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {
            if (timestamps[i] != current_time) {
                std::cout << "Error at " << current_time << " expected " << current_time << " acutal " << timestamps[i]  << std::endl;
                aku_close_cursor(cursor);
                return last;
            }
            current_time++;
            last = timestamps[i];
            counter++;
            if (counter % mod == 0) {
                std::cout << counter << "..." << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
    }
    aku_close_cursor(cursor);
    return last;
}

int main(int cnt, const char** args)
{
    aku_initialize();

    // Cleanup
    delete_storage();

    // Create database
    apr_status_t result = aku_create_database(DB_NAME, DB_PATH, DB_PATH, DB_SIZE, nullptr, nullptr, nullptr, nullptr);
    if (result != APR_SUCCESS) {
        std::cout << "Error in new_storage" << std::endl;
        return (int)result;
    }

    aku_FineTuneParams params;
    params.debug_mode = 0;
    params.max_late_write = 10000;
    auto db = aku_open_database(DB_META_FILE, params);
    boost::timer timer;

    auto reader_fn = [&db]() {
        boost::timer timer;
        aku_TimeStamp top = 0u;
        uint64_t counter = 0;
        // query last elements from database
        while (true) {
            top = query_database(db, top, AKU_MAX_TIMESTAMP, counter, timer, 1000000);
            if (top == NUM_ITERATIONS - 1) {
                break;
            }
        }
    };

    std::thread reader_thread(reader_fn);

    int busy_count = 0;
    for(uint64_t i = 0; i < NUM_ITERATIONS; i++) {
        aku_MemRange memr;
        memr.address = (void*)&i;
        memr.length = sizeof(i);
        aku_Status status = aku_add_sample(db, 1, i, memr);
        if (status == AKU_SUCCESS) {
            if (i % 1000000 == 0) {
                std::cout << i << "---" << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        } else if (status == AKU_EBUSY) {
            busy_count++;
            i--;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } else {
            std::cout << "aku_add_sample error " << aku_error_message(status) << std::endl;
            break;
        }
    }
    std::cout << "Busy count = " << busy_count << std::endl;

    reader_thread.join();

    aku_SearchStats search_stats;

    aku_global_search_stats(&search_stats, true);
    print_search_stats(search_stats);

    aku_close_database(db);

    delete_storage();

    return 0;
}

