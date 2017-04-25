#include <iostream>
#include <cassert>
#include <random>
#include <tuple>
#include <map>
#include <vector>
#include <algorithm>
#include <memory>
#include <thread>

#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <apr_mmap.h>
#include <apr_general.h>
#include <sys/time.h>

#include "akumuli.h"

using namespace std;

static int DB_SIZE = 8;
static u64 NUM_ITERATIONS = 10*1000*1000ul;
static int CHUNK_SIZE = 50000;

//static const char* DB_NAME = "db";
//static const char* DB_PATH = "";
static const char* DB_META_FILE = "/tmp/testdb.akumuli";

class Timer
{
public:
    Timer() { gettimeofday(&_start_time, nullptr); }
    void   restart() { gettimeofday(&_start_time, nullptr); }
    double elapsed() const {
        timeval curr;
        gettimeofday(&curr, nullptr);
        return double(curr.tv_sec - _start_time.tv_sec) +
               double(curr.tv_usec - _start_time.tv_usec)/1000000.0;
    }
private:
    timeval _start_time;
};

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
    str << R"({ "select": "cpu", )";
    str << R"("range": { "from": ")" << ts2str(begin)
        << R"(", "to": ")" << ts2str(end)
        << R"("}})";
    return str.str();
}

void delete_storage() {
    aku_remove_database(DB_META_FILE, true);
}

bool query_database_forward(aku_Database* db, aku_Timestamp begin, aku_Timestamp end, u64& counter, Timer& timer, u64 mod) {
    const unsigned int NUM_ELEMENTS = 1000;
    std::string query = build_query(begin, end);
    aku_Session* session = aku_create_session(db);
    aku_Cursor* cursor = aku_query(session, query.c_str());
    aku_Timestamp current_time = begin;
    size_t cursor_ix = 0;
    int nerrors = 0;
    while(!aku_cursor_is_done(cursor)) {
        aku_Status err = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor, &err)) {
            std::cout << aku_error_message(err) << std::endl;
            return false;
        }
        aku_Sample samples[NUM_ELEMENTS];
        int n_entries = aku_cursor_read(cursor, samples, NUM_ELEMENTS);
        for (int i = 0; i < n_entries; i++) {

            if (samples[i].timestamp != current_time) {
                std::cout << "Error at " << cursor_ix << " expected ts " << current_time << " acutal ts " << samples[i].timestamp  << std::endl;
                current_time = samples[i].timestamp;
                if (nerrors++ == 10) {
                    return false;
                }
            } else {
                double dvalue = samples[i].payload.float64;
                double dexpected = (current_time) + 1;
                if (std::abs(dvalue - dexpected) > 0.000001) {
                    std::cout << "Error at " << cursor_ix << " expected value " << dexpected << " acutal value " << dvalue  << std::endl;
                    if (nerrors++ == 10) {
                        return false;
                    }
                }
            }
            current_time++;
            counter++;
            if (counter % mod == 0) {
                std::cout << counter << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
            cursor_ix++;
        }
    }
    aku_cursor_close(cursor);
    aku_destroy_session(session);
    auto last_ts =  end - 1;
    if (current_time != last_ts) {
        std::cout << "some values lost (1), actual timestamp: " << current_time << ", expected timestamp: " << last_ts << std::endl;
        throw std::runtime_error("values lost");
    }
    if (cursor_ix > 1000) {
        std::cout << "cursor_ix = " << cursor_ix << std::endl;
    }
    return true;
}

void print_storage_stats(aku_StorageStats& ss) {
    std::cout << ss.n_entries << " elenents in" << std::endl
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

enum Mode {
    NONE,
    CREATE,
    DELETE,
    READ
};

//! Generate time-series from random walk
struct RandomGen {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>  distribution;

    RandomGen(double mean, double stddev)
        : generator(randdev())
        , distribution(mean, stddev)
    {
    }

    int generate() {
        return static_cast<int>(abs(distribution(generator)));
    }
};

//! Generate time-series from random walk
struct RandomWalk {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>    distribution;
    size_t                              N;
    std::vector<double>                 values;

    RandomWalk(double start, double mean, double stddev, size_t N)
        : generator(randdev())
        , distribution(mean, stddev)
        , N(N)
    {
        values.resize(N, start);
    }

    double generate(aku_ParamId id) {
        values.at(id) += distribution(generator);
        return values.at(id);
    }

    void add_anomaly(aku_ParamId id, double value) {
        values.at(id) += value;
    }
};

Mode read_cmd(int cnt, const char** args) {
    if (cnt < 2) {
        return NONE;
    }
    if (cnt == 4) {
        DB_SIZE        = boost::lexical_cast<int>(args[2]);
        NUM_ITERATIONS = boost::lexical_cast<u64>(args[3]);

        if (NUM_ITERATIONS >= 10000000000) {
            std::cout << "NUM_ITERATIONS set too large" << std::endl;
            throw std::runtime_error("Bad command line parameters");
        }
    }
    if (std::string(args[1]) == "create") {
        return CREATE;
    }
    if (std::string(args[1]) == "read") {
        return READ;
    }
    if (std::string(args[1]) == "delete") {
        return DELETE;
    }
    std::cout << "Invalid command line" << std::endl;
    throw std::runtime_error("Bad command line parameters");
}

void logger_(aku_LogLevel level, const char * msg) {
    if (level == AKU_LOG_ERROR) {
        aku_console_logger(level, msg);
    }
}

int main(int, const char**) {

    aku_initialize(nullptr, logger_);

    // Delete database
    //

    aku_Status status = aku_remove_database("/tmp/testdb.akumuli", true);
    std::cout << "Remove old database, status: " << aku_error_message(status) << std::endl;

    // Create database
    //

    status = aku_create_database("testdb", "/tmp", "/tmp", 2);
    if (status != AKU_SUCCESS) {
        std::cerr << "Can't create database" << std::endl;
    }

    // Open database
    //

    aku_FineTuneParams params = {};

    auto db = aku_open_database(DB_META_FILE, params);

    auto worker = [db](aku_ParamId begin, aku_ParamId end) {
        auto session = aku_create_session(db);
        Timer timer;
        RandomWalk rwalk(10.0, 0.0, 0.002, 10000);
        std::vector<aku_ParamId> ids;
        // Genearate all ids
        for (aku_ParamId it = begin; it < end; it++) {
            char buffer[0x100];
            int nchars = sprintf(buffer, "cpu id=%d", static_cast<int>(it));
            aku_Sample sample;
            aku_series_to_param_id(session, buffer, buffer + nchars, &sample);
            ids.push_back(sample.paramid);
        }
        size_t load = NUM_ITERATIONS / ids.size();
        // Generate data
        for(u64 i = 0; i < NUM_ITERATIONS; i++) {
            aku_Sample sample;
            //char buffer[100];
            // =series=
            sample.paramid = ids.at(i / load);

            // =timestamp=
            sample.timestamp = i;

            // =payload=
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.payload.float64 = rwalk.generate(sample.paramid);

            aku_Status status = aku_write(session, &sample);

            if (status != AKU_SUCCESS) {
                std::cout << "Error at " << i << std::endl;
                std::terminate();
            }
            if (i % 1000000 == 0) {
                std::cout << i << " " << timer.elapsed() << "s" << std::endl;
                timer.restart();
            }
        }
        aku_destroy_session(session);
    };

    std::thread th0(std::bind(worker, 0, 1000));
    std::thread th1(std::bind(worker, 1000, 2000));
    //std::thread th2(std::bind(worker, 3000, 4000));
    //std::thread th3(std::bind(worker, 4000, 5000));
    th0.join();
    th1.join();
    //th2.join();
    //th3.join();

    //aku_SearchStats search_stats = {};
    //aku_StorageStats storage_stats = {};
    //aku_global_storage_stats(db, &storage_stats);
    //print_storage_stats(storage_stats);

    // Search
    std::cout << "Sequential access" << std::endl;
    u64 counter = 0;

    Timer timer;

    if (!query_database_forward(db, std::numeric_limits<aku_Timestamp>::min(),
                                NUM_ITERATIONS-1,
                                counter,
                                timer,
                                1000000))
    {
        return 2;
    }

    //aku_global_search_stats(&search_stats, true);
    //print_search_stats(search_stats);

    // Random access
    std::cout << "Prepare test data" << std::endl;
    std::vector<std::pair<aku_Timestamp, aku_Timestamp>> ranges;
    for (aku_Timestamp i = 1u; i < (aku_Timestamp)NUM_ITERATIONS/CHUNK_SIZE; i++) {
        aku_Timestamp j = (i - 1)*CHUNK_SIZE;
        int count = 5;
        for (int d = 0; d < count; d++) {
            int r = std::rand() % CHUNK_SIZE;
            int k = j + r;
            ranges.push_back(std::make_pair(k, k+1));
        }
    }

    std::random_shuffle(ranges.begin(), ranges.end());

    std::cout << "Random access" << std::endl;
    counter = 0;
    timer.restart();
    for(auto range: ranges) {
        if (!query_database_forward(db, range.first, range.second, counter, timer, 1000)) {
            return 3;
        }
    }
    //aku_global_search_stats(&search_stats, true);
    //print_search_stats(search_stats);

    aku_close_database(db);
    return 0;
}

