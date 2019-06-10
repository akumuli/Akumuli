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
        std::cout << "INFO: " << msg << std::endl;
        break;
    case AKU_LOG_TRACE:
        //std::cerr << "trace: " << msg << std::endl;
        break;
    };
}

int main() {
    apr_initialize();

    Akumuli::Logger::set_logger(console_logger);


    const double start = 0.0;
    const double inc = 0.1;
    const double factor = 1.1;
    const int N = 1000000;

    std::cout << "NBTree[w,r]\tIOVec[w,r]" << std::endl;
    double t[4];
    for (int o = 0; o < 10; o++)
    {
        {
            std::unique_ptr<NBTreeLeaf> leaf(new NBTreeLeaf(42, EMPTY_ADDR, 0));
            int leaf_append_cnt = 0;
            LogicAddr last_addr = EMPTY_ADDR;
            LogicAddr first_addr = EMPTY_ADDR;
            auto bstore = BlockStoreBuilder::create_memstore([&](LogicAddr a) {
                if (first_addr == EMPTY_ADDR) {
                    first_addr = a;
                }
                leaf_append_cnt++;
                last_addr = a;
            });
            Timer tm;
            double x = start;
            for (int i = 0; i < N; i++) {
                x += inc;
                x *= factor;
                auto status = leaf->append(i, x);
                if (status == AKU_EOVERFLOW) {
                    LogicAddr addr;
                    std::tie(status, addr) = leaf->commit(bstore);
                    if (status != AKU_SUCCESS) {
                        std::cout << "Failed to commit leaf" << std::endl;
                        std::abort();
                    }
                    if (addr != last_addr) {
                        std::cout << "Unexpected address " << addr << " returned, " << last_addr << " expected" << std::endl;
                        std::abort();
                    }
                    leaf.reset(new NBTreeLeaf(42, EMPTY_ADDR, 0));
                }
            }
            t[0] = tm.elapsed();

            // Read back
            std::vector<aku_Timestamp> ts;
            std::vector<double> xs;
            ts.reserve(5000);
            xs.reserve(5000);

            tm.restart();

            for (LogicAddr addr = first_addr; addr < last_addr; addr++) {
                NBTreeLeaf rdleaf(bstore, addr);
                auto status = rdleaf.read_all(&ts, &xs);
                if(status != AKU_SUCCESS) {
                    std::cout << "Failed to read block " << addr << std::endl;
                    std::abort();
                }
            }

            t[1] = tm.elapsed();
        }
        {
            std::unique_ptr<IOVecLeaf> leaf(new IOVecLeaf(42, EMPTY_ADDR, 0));
            int leaf_append_cnt = 0;
            LogicAddr last_addr = EMPTY_ADDR;
            LogicAddr first_addr = EMPTY_ADDR;
            auto bstore = BlockStoreBuilder::create_memstore([&](LogicAddr a) {
                if (first_addr == EMPTY_ADDR) {
                    first_addr = a;
                }
                leaf_append_cnt++;
                last_addr = a;
            });
            Timer tm;
            double x = start;
            for (int i = 0; i < N; i++) {
                x += inc;
                x *= factor;
                auto status = leaf->append(i, x);
                if (status == AKU_EOVERFLOW) {
                    LogicAddr addr;
                    std::tie(status, addr) = leaf->commit(bstore);
                    if (status != AKU_SUCCESS) {
                        std::cout << "Failed to commit leaf" << std::endl;
                        std::abort();
                    }
                    if (addr != last_addr) {
                        std::cout << "Unexpected address " << addr << " returned, " << last_addr << " expected" << std::endl;
                        std::abort();
                    }
                    leaf.reset(new IOVecLeaf(42, EMPTY_ADDR, 0));
                }
            }
            t[2] = tm.elapsed();

            // Read back
            std::vector<aku_Timestamp> ts;
            std::vector<double> xs;
            ts.reserve(5000);
            xs.reserve(5000);

            tm.restart();

            for (LogicAddr addr = first_addr; addr < last_addr; addr++) {
                NBTreeLeaf rdleaf(bstore, addr);
                auto status = rdleaf.read_all(&ts, &xs);
                if(status != AKU_SUCCESS) {
                    std::cout << "Failed to read block " << addr << std::endl;
                    std::abort();
                }
            }

            t[3] = tm.elapsed();
        }
        std::cout << t[0] << ", " << t[1] << "\t" << t[2] << ", " << t[3] << std::endl;
    }
    return 0;
}
