#include <iostream>
#include <cassert>
#include <random>
#include <tuple>
#include <map>
#include <vector>
#include <algorithm>
#include <memory>
#include <thread>

#include <boost/timer.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <apr_mmap.h>
#include <apr_general.h>

#include "akumuli.h"

using namespace std;

const int DB_SIZE = 3;
const int NUM_ITERATIONS = 100*1000*1000;

const char* DB_NAME = "test";
const char* DB_PATH = "./test";
const char* DB_META_FILE = "./test/test.akumuli";

uint64_t reader_n_busy = 0ul;

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

aku_TimeStamp query_database_backward(aku_Database* db, aku_TimeStamp begin, aku_TimeStamp end, uint64_t& counter, boost::timer& timer, uint64_t mod) {
    const int NUM_ELEMENTS = 1000;
    aku_ParamId params[] = {42};
    aku_SelectQuery* query = aku_make_select_query( end
                                                  , begin
                                                  , 1
                                                  , params);
    aku_Cursor* cursor = aku_select(db, query);
    aku_TimeStamp current_time = end;
    aku_TimeStamp last = begin;
    bool last_initialized = false;
    while(!aku_cursor_is_done(cursor)) {
        int err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            aku_close_cursor(cursor);
            if (err == AKU_EBUSY) { // OK
                reader_n_busy++;
                return last;
            } else {                           // Critical
                std::cout << aku_error_message(err) << std::endl;
                throw std::runtime_error(aku_error_message(err));
            }
        }
        aku_TimeStamp timestamps[NUM_ELEMENTS];
        aku_ParamId paramids[NUM_ELEMENTS];
        aku_PData pointers[NUM_ELEMENTS];
        uint32_t lengths[NUM_ELEMENTS];
        int n_entries = aku_cursor_read_columns(cursor, timestamps, paramids, pointers, lengths, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {
            if (last_initialized) {
                if (timestamps[i] != current_time) {
                    std::cout << "(BW) Bad ts at " << current_time << " expected " << current_time << " acutal " << timestamps[i] << std::endl;
                    aku_close_cursor(cursor);
                    return last;
                }
                if (paramids[i] != 42) {
                    std::cout << "(BW) Bad id at " << current_time << " expected " << 42 << " acutal " << paramids[i] << std::endl;
                    aku_close_cursor(cursor);
                    return last;

                }
                if (lengths[i] != 8) {
                    std::cout << "(BW) Bad len at " << current_time << " expected 8 acutal " << lengths[i] << std::endl;
                    aku_close_cursor(cursor);
                    return last;

                }
                uint64_t pvalue = *(uint64_t*)pointers[i];
                if (pvalue != (current_time << 2)) {
                    std::cout << "(BW) Bad value at " << current_time << " expected " << (current_time << 2) << " acutal " << pvalue << std::endl;
                    aku_close_cursor(cursor);
                    return last;

                }
            }
            if (!last_initialized) {
                last = timestamps[i];
                current_time = last;
                last_initialized = true;
            }
            current_time--;
            counter++;
            if (counter % mod == 0) {
                std::cout << counter << "..." << timer.elapsed() << "s (bw)" << std::endl;
                timer.restart();
            }
        }
    }
    aku_close_cursor(cursor);
    return last;
}

aku_TimeStamp query_database_forward(aku_Database* db, aku_TimeStamp begin, aku_TimeStamp end, uint64_t& counter, boost::timer& timer, uint64_t mod) {
    const int NUM_ELEMENTS = 1000;
    aku_ParamId params[] = {42};
    aku_SelectQuery* query = aku_make_select_query( begin
                                                  , end
                                                  , 1, params);
    aku_Cursor* cursor = aku_select(db, query);
    aku_TimeStamp current_time = begin;
    aku_TimeStamp last = begin;
    while(!aku_cursor_is_done(cursor)) {
        int err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            aku_close_cursor(cursor);
            if (err == AKU_EBUSY) { // OK
                reader_n_busy++;
                return last;
            } else {                           // Critical
                std::cout << aku_error_message(err) << std::endl;
                throw std::runtime_error(aku_error_message(err));
            }
        }
        aku_TimeStamp timestamps[NUM_ELEMENTS];
        aku_ParamId paramids[NUM_ELEMENTS];
        aku_PData pointers[NUM_ELEMENTS];
        uint32_t lengths[NUM_ELEMENTS];
        int n_entries = aku_cursor_read_columns(cursor, timestamps, paramids, pointers, lengths, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {
            if (timestamps[i] != current_time) {
                std::cout << "(FW) Bad ts at " << current_time << " expected " << current_time << " acutal " << timestamps[i] << std::endl;
                aku_close_cursor(cursor);
                return last;
            }
            if (paramids[i] != 42) {
                std::cout << "(FW) Bad id at " << current_time << " expected " << 42 << " acutal " << paramids[i] << std::endl;
                aku_close_cursor(cursor);
                return last;

            }
            if (lengths[i] != 8) {
                std::cout << "(FW) Bad len at " << current_time << " expected 8 acutal " << lengths[i] << std::endl;
                aku_close_cursor(cursor);
                return last;

            }
            uint64_t pvalue = *(uint64_t*)pointers[i];
            if (pvalue != (current_time << 2)) {
                std::cout << "(FW) Bad value at " << current_time << " expected " << (current_time << 2) << " acutal " << pvalue << std::endl;
                aku_close_cursor(cursor);
                return last;

            }
            current_time++;
            last = timestamps[i];
            counter++;
            if (counter % mod == 0) {
                std::cout << counter << "..." << timer.elapsed() << "s (fw)" << std::endl;
                timer.restart();
            }
        }
    }
    aku_close_cursor(cursor);
    return last;
}

int main(int cnt, const char** args)
{
    aku_initialize(nullptr);

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
    auto db = aku_open_database(DB_META_FILE, params);
    boost::timer timer;

    auto reader_fn_bw = [&db]() {
        boost::timer timer;
        aku_TimeStamp top = 0u;
        uint64_t counter = 0;
        uint64_t query_counter = 0;
        // query last elements from database
        while (true) {
            top = query_database_backward(db, top, AKU_MAX_TIMESTAMP, counter, timer, 1000000);
            query_counter++;
            if (top == NUM_ITERATIONS - 1) {
                std::cout << "query_counter=" << query_counter << std::endl;
                break;
            }
        }
    };

    auto reader_fn_fw = [&db]() {
        boost::timer timer;
        aku_TimeStamp top = 0u;
        uint64_t counter = 0;
        uint64_t query_counter = 0;
        // query last elements from database
        while (true) {
            top = query_database_forward(db, top, AKU_MAX_TIMESTAMP, counter, timer, 1000000);
            query_counter++;
            if (top >= (NUM_ITERATIONS - 20001)) {
                std::cout << "query_counter=" << query_counter << std::endl;
                break;
            }
        }
    };

    std::thread fw_reader_thread(reader_fn_fw);
    std::thread bw_reader_thread(reader_fn_bw);

    int writer_n_busy = 0;
    for(uint64_t ts = 0; ts < NUM_ITERATIONS; ts++) {
        aku_MemRange memr;
        auto param_id = 42u;
        auto value = ts << 2;
        memr.address = (void*)&value;
        memr.length = sizeof(value);
        aku_Status status = aku_write(db, param_id, ts, memr);
        if (status == AKU_SUCCESS) {
            if (ts % 1000000 == 0) {
                std::cout << ts << "---" << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        } else if (status == AKU_EBUSY) {
            writer_n_busy++;
            status = aku_write(db, param_id, ts, memr);
        }
        if (status != AKU_SUCCESS) {
            std::cout << "aku_add_sample error " << aku_error_message(status) << std::endl;
            break;
        }
    }
    std::cout << "Writer busy count = " << writer_n_busy << std::endl;

    fw_reader_thread.join();
    bw_reader_thread.join();

    std::cout << "Reader busy count = " << reader_n_busy << std::endl;

    aku_SearchStats search_stats;

    aku_global_search_stats(&search_stats, true);
    print_search_stats(search_stats);

    aku_close_database(db);

    delete_storage();

    return 0;
}

