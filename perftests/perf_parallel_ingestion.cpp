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

u64 reader_n_busy = 0ul;

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

int format_timestamp(u64 ts, char* buffer) {
    auto fractional = static_cast<int>(ts %  1000000000);  // up to 9 decimal digits
    auto seconds = static_cast<int>(ts / 1000000000);      // two seconds digits
    return sprintf(buffer, "20150102T0304%02d.%09d", seconds, fractional);
}

std::string ts2str(u64 ts) {
    char buffer[0x100];
    auto len = format_timestamp(ts, buffer);
    return std::string(buffer, buffer+len);
}

std::string build_query(u64 begin, u64 end) {
    std::stringstream str;
    str << R"({ "sample": "all", )";
    str << R"("range": { "from": ")" << ts2str(begin)
        << R"(", "to": ")" << ts2str(end)
        << R"("}})";
    return str.str();
}

aku_Timestamp query_database_backward(aku_Database* db, aku_Timestamp begin, aku_Timestamp end, u64& counter, boost::timer& timer, u64 mod) {
    const int NUM_ELEMENTS = 1000;
    std::string query = build_query(begin, end);
    aku_Cursor* cursor = aku_query(db, query.c_str());
    aku_Timestamp current_time = end;
    aku_Timestamp last = begin;
    bool last_initialized = false;
    while(!aku_cursor_is_done(cursor)) {
        aku_Status err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            aku_cursor_close(cursor);
            if (err == AKU_EBUSY) { // OK
                reader_n_busy++;
                return last;
            } else {                           // Critical
                std::cout << aku_error_message(err) << std::endl;
                throw std::runtime_error(aku_error_message(err));
            }
        }
        aku_Sample samples[NUM_ELEMENTS];
        int n_entries = aku_cursor_read(cursor, samples, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {
            if (last_initialized) {
                if (samples[i].timestamp != current_time) {
                    std::cout << "(BW) Bad ts at " << current_time << " expected " << current_time
                              << " acutal " << samples[i].timestamp << std::endl;
                    aku_cursor_close(cursor);
                    return last;
                }
                if (samples[i].paramid != 42) {
                    std::cout << "(BW) Bad id at " << current_time << " expected " << 42
                              << " acutal " << samples[i].paramid << std::endl;
                    aku_cursor_close(cursor);
                    return last;

                }
            }
            if (!last_initialized) {
                last = samples[i].timestamp;
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
    aku_cursor_close(cursor);
    return last;
}

aku_Timestamp query_database_forward(aku_Database* db, aku_Timestamp begin, aku_Timestamp end, u64& counter, boost::timer& timer, u64 mod) {
    const int NUM_ELEMENTS = 1000;
    std::string query = build_query(end, begin);
    aku_Cursor* cursor = aku_query(db, query.c_str());
    aku_Timestamp current_time = begin;
    aku_Timestamp last = begin;
    while(!aku_cursor_is_done(cursor)) {
        aku_Status err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            aku_cursor_close(cursor);
            if (err == AKU_EBUSY) { // OK
                reader_n_busy++;
                return last;
            } else {                           // Critical
                std::cout << aku_error_message(err) << std::endl;
                throw std::runtime_error(aku_error_message(err));
            }
        }
        aku_Sample samples[NUM_ELEMENTS];
        int n_entries = aku_cursor_read(cursor, samples, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {
            if (samples[i].timestamp != current_time) {
                std::cout << "(FW) Bad ts at " << current_time << " expected " << current_time
                          << " acutal " << samples[i].timestamp << std::endl;
                aku_cursor_close(cursor);
                return last;
            }
            if (samples[i].paramid != 42) {
                std::cout << "(FW) Bad id at " << current_time << " expected " << 42
                          << " acutal " << samples[i].paramid << std::endl;
                aku_cursor_close(cursor);
                return last;

            }
            current_time++;
            last = samples[i].timestamp;
            counter++;
            if (counter % mod == 0) {
                std::cout << counter << "..." << timer.elapsed() << "s (fw)" << std::endl;
                timer.restart();
            }
        }
    }
    aku_cursor_close(cursor);
    return last;
}

int main(int cnt, const char** args)
{
    aku_initialize(nullptr);

    // Cleanup
    delete_storage();

    // Create database
    apr_status_t result = aku_create_database(DB_NAME, DB_PATH, DB_PATH, DB_SIZE, nullptr);
    if (result != APR_SUCCESS) {
        std::cout << "Error in new_storage" << std::endl;
        return (int)result;
    }

    aku_FineTuneParams params = {};
    params.debug_mode = 0;
    auto db = aku_open_database(DB_META_FILE, params);
    boost::timer timer;

    auto reader_fn_bw = [&db]() {
        boost::timer timer;
        aku_Timestamp top = 0u;
        u64 counter = 0;
        u64 query_counter = 0;
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
        aku_Timestamp top = 0u;
        u64 counter = 0;
        u64 query_counter = 0;
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
    for(u64 ts = 0; ts < NUM_ITERATIONS; ts++) {
        u64 k = ts + 2;
        double value = 0.0001*k;
        aku_ParamId id = ts & 0xF;
        aku_Status status = aku_write_double_raw(db, id, ts, value);
        if (status == AKU_SUCCESS) {
            if (ts % 1000000 == 0) {
                std::cout << ts << "---" << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        } else if (status == AKU_EBUSY) {
            writer_n_busy++;
            status = aku_write_double_raw(db, id, ts, value);
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

