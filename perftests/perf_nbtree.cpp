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
#include "util.h"


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
        //std::make_tuple(1024*1024, paths[0]),
        //std::make_tuple(1024*1024, paths[1]),
        //std::make_tuple(1024*1024, paths[2]),
        //std::make_tuple(1024*1024, paths[3]),
    };

    FixedSizeFileStorage::create(metapath, volumes);

    auto bstore = FixedSizeFileStorage::open(metapath, paths);

    std::vector<std::shared_ptr<NBTreeExtentsList>> trees;

    //const int numids = 1;
    const int numids = 10000;
    //const int numids = 100;

    const int N = 100000000;
    //const int N = 20000000;

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

    auto writer = [&](size_t begin, size_t end) {
        auto fn = [&]() {
            Timer tm;
            Timer total;
            size_t nsamples = 0;
            size_t nbatch = end - begin;
            for (int i = 1; i < (N + 1); i++) {
                aku_Timestamp ts = nsamples;//static_cast<aku_Timestamp>(i);
                double value = i;
                aku_ParamId id = begin + (i % nbatch);
                if (trees[id]->append(ts, value) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
                    flush_needed = true;
                    cvar.notify_one();
                }
                nsamples++;
                if (nsamples % 1000000 == 0) {
                    std::cout << i << "\t" << tm.elapsed() << " sec" << std::endl;
                    tm.restart();
                }
            }
            std::cout << "Write time: " << total.elapsed() << "s" << std::endl;
        };
        return fn;
    };

    std::thread th1(writer(0, numids/4));
    std::thread th2(writer(numids/4, numids/4*2));
    std::thread th3(writer(numids/4*2, numids/4*3));
    std::thread th4(writer(numids/4*3, numids));
    th1.join();
    th2.join();
    th3.join();
    th4.join();

    {   // stop flush thread
        done = true;
        cvar.notify_one();
    }

    Timer total;
    for (size_t i = 0; i < trees.size(); i++) {
        trees[i]->close();
    }
    std::cout << "Commit time: " << total.elapsed() << "s" << std::endl;

    return 0;
}
