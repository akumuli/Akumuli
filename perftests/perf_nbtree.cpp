// C++ headers
#include <iostream>
#include <vector>

// Lib headers
#include <apr.h>
#include <sys/time.h>

// App headers
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"
#include "log_iface.h"


using namespace Akumuli::StorageEngine;

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

static void console_logger(aku_LogLevel lvl, const char* msg) {
    switch(lvl) {
    case AKU_LOG_ERROR:
        std::cerr << "ERROR: " << msg << std::endl;
        break;
    case AKU_LOG_INFO:
        std::cout << "Info: " << msg << std::endl;
        break;
    case AKU_LOG_TRACE:
        //std::cerr << "trace: " << msg << std::endl;
        break;
    };
}

int main() {
    apr_initialize();

    Akumuli::Logger::set_logger(console_logger);

    // Create volumes
    std::string metapath = "/tmp/metavol.db";
    std::vector<std::string> paths = {
        "/tmp/volume0.db",
        "/tmp/volume1.db",
        //"/tmp/volume2.db",
        //"/tmp/volume3.db",
    };
    std::vector<std::tuple<u32, std::string>> volumes {
        std::make_tuple(1024*1024, paths[0]),
        std::make_tuple(1024*1024, paths[1]),
        //std::make_tuple(1024*1024, paths[2]),
        //std::make_tuple(1024*1024, paths[3]),
    };

    FixedSizeFileStorage::create(metapath, volumes);

    auto bstore = FixedSizeFileStorage::open(metapath, paths);

    std::vector<std::shared_ptr<NBTreeExtentsList>> trees;
    const int numids = 10000;
    //const int numids = 1;
    for (int i = 0; i < numids; i++) {
        auto id = static_cast<aku_ParamId>(i);
        std::vector<LogicAddr> empty;
        auto ext = std::make_shared<NBTreeExtentsList>(id, empty, bstore);
        trees.push_back(std::move(ext));
    }

    bool flush_needed = false;
    bool done = false;
    std::condition_variable cvar;
    std::mutex lock;
    auto flush_fn = [bstore, &flush_needed, &done, &cvar, &lock]() {
        while (!done) {
            std::unique_lock<std::mutex> g(lock);
            cvar.wait(g);
            if (flush_needed) {
                bstore->flush();
                flush_needed = false;
            }
        }
    };

    std::thread flush_thread(flush_fn);
    flush_thread.detach();

    const int N = 100000000;

    Timer tm;
    Timer total;
    size_t rr = 0;
    size_t nsamples = 0;
    std::vector<aku_ParamId> ids;
    const int nextracted = std::min(10, numids);
    for (int i = 1; i < (N+1); i++) {
        aku_Timestamp ts = static_cast<aku_Timestamp>(i);
        double value = i;
        if (rr % 10000 == 0) {
            rr = static_cast<size_t>(rand());
        }
        aku_ParamId id = rr++ % trees.size();
        if (trees[id]->append(ts, value) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            flush_needed = true;
            cvar.notify_one();
        }
        if (nsamples < static_cast<size_t>(nextracted)) {
            ids.push_back(id);
        } else {
            int rix = rand() % nsamples;
            if (rix < nextracted) {
                ids[rix] = id;
            }
        }
        nsamples++;
        if (i % 1000000 == 0) {
            std::cout << i << "\t" << tm.elapsed() << " sec" << std::endl;
            tm.restart();
        }
    }

    {   // stop flush thread
        done = true;
        cvar.notify_one();
    }

    std::cout << "Write time: " << total.elapsed() << "s" << std::endl;

    for (auto id: ids) {
        total.restart();
        auto it = trees[id]->search(N+1, 0);
        double sum = 0;
        size_t total_sum = 0;
        aku_Status status = AKU_SUCCESS;
        std::vector<aku_Timestamp> ts(0x1000, 0);
        std::vector<double> xs(0x1000, 0.0);
        while(status == AKU_SUCCESS) {
            size_t sz;
            std::tie(status, sz) = it->read(ts.data(), xs.data(), 0x1000);
            total_sum += sz;
            for (size_t i = 0; i < sz; i++) {
                sum += xs[i];
            }
        }
        std::cout << "From id: " << id << " n: " << total_sum << " sum: "
                  << sum << " calculated in " << total.elapsed() << "s" << std::endl;
        total.restart();
        it = trees[id]->aggregate(N+1, 0, NBTreeAggregation::SUM);
        size_t sz;
        std::tie(status, sz) = it->read(ts.data(), xs.data(), 0x1000);
        if (sz != 1) {
            std::cout << "Failure at id = " << id << std::endl;
        }
        if (std::abs(sum - xs.at(0)) > .0001) {
            std::cout << "Failure at id = " << id << ", sums didn't match "
                      << total_sum << " != " << xs.at(0) << std::endl;
        }
        std::cout << "From id: " << id << " n: " << total_sum << " sum: "
                  << xs.at(0) << " aggregated in " << total.elapsed() << "s" << std::endl;
        it = trees[id]->aggregate(N+1, 0, NBTreeAggregation::CNT);
        std::tie(status, sz) = it->read(ts.data(), xs.data(), 0x1000);
        if (sz != 1) {
            std::cout << "Failure at id = " << id << std::endl;
        }
        std::cout << "From id: " << id << " n: " << xs.at(0)
                  << " aggregated in " << total.elapsed() << "s" << std::endl;
    }

    // Test recovery
    std::vector<std::vector<LogicAddr>> rescue_points;
    for (int i = 0; i < numids; i++) {
        rescue_points.push_back(trees[i]->get_roots());
    }
    total.restart();
    std::vector<std::shared_ptr<NBTreeExtentsList>> tmptrees;
    for (int i = 0; i < numids; i++) {
        auto id = static_cast<aku_ParamId>(i);
        auto ext = std::make_shared<NBTreeExtentsList>(id, rescue_points[i], bstore);
        tmptrees.push_back(std::move(ext));
    }
    size_t total_cnt = 0;
    for (int i = 0; i < numids; i++) {
        auto it = tmptrees[i]->aggregate(N+1, 0, NBTreeAggregation::CNT);
        aku_Status status;
        size_t sz;
        aku_Timestamp ts;
        double val;
        std::tie(status, sz) = it->read(&ts, &val, 0x1);
        if (sz == 1) {
            total_cnt += static_cast<size_t>(val);
        }
    }
    std::cout << "Recovery completed in " << total.elapsed() << " sec" << std::endl;
    std::cout << "n = " << total_cnt << " elements" << std::endl;
    tmptrees.clear();
    // end test recovery

    size_t orig_total_cnt = 0;
    for (int i = 0; i < numids; i++) {
        auto it = trees[i]->aggregate(N+1, 0, NBTreeAggregation::CNT);
        aku_Status status;
        size_t sz;
        aku_Timestamp ts;
        double val;
        std::tie(status, sz) = it->read(&ts, &val, 0x1);
        if (sz == 1) {
            orig_total_cnt += static_cast<size_t>(val);
        }
    }
    std::cout << "n (original) = " << orig_total_cnt << " elements" << std::endl;

    total.restart();
    for (size_t i = 0; i < trees.size(); i++) {
        trees[i]->close();
    }

    std::cout << "Commit time: " << total.elapsed() << "s" << std::endl;

    return 0;
}
