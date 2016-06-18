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

int main() {
    apr_initialize();

    // Create volumes
    std::string metapath = "/tmp/metavol.db";
    std::vector<std::string> paths = {
        "/tmp/volume0.db",
        "/tmp/volume1.db",
    };
    std::vector<std::tuple<u32, std::string>> volumes {
        std::make_tuple(1024*1024, paths[0]),
        std::make_tuple(1024*1024, paths[1])
    };

    FixedSizeFileStorage::create(metapath, volumes);

    auto bstore = FixedSizeFileStorage::open(metapath, paths);

    std::vector<std::shared_ptr<NBTreeExtentsList>> trees;
    const int numids = 10000;
    for (int i = 0; i < numids; i++) {
        auto id = static_cast<aku_ParamId>(i);
        std::vector<LogicAddr> empty;
        auto ext = std::make_shared<NBTreeExtentsList>(id, empty, bstore);
        trees.push_back(std::move(ext));
    }

    const int N = 500000000;

    Timer tm;
    Timer total;
    size_t rr = 0;
    size_t flush = 0;
    size_t nsamples = 0;
    std::vector<aku_ParamId> ids;
    for (int i = 1; i < (N+1); i++) {
        aku_Timestamp ts = static_cast<aku_Timestamp>(i);
        double value = i;
        if (rr % 10000 == 0) {
            rr = static_cast<size_t>(rand());
        }
        aku_ParamId id = rr++ % trees.size();
        if (trees[id]->append(ts, value)) {
            flush++;
            if (flush % trees.size() == 0) {
                std::cout << "About to call flush" << std::endl;
                bstore->flush();
            }
        }
        if (nsamples < 10) {
            ids.push_back(id);
        } else {
            int rix = rand() % nsamples;
            if (rix < 10) {
                ids[rix] = id;
            }
        }
        nsamples++;
        if (i % 1000000 == 0) {
            std::cout << i << "\t" << tm.elapsed() << " sec" << std::endl;
            tm.restart();
        }
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
    }

    total.restart();
    for (size_t i = 0; i < trees.size(); i++) {
        trees[i]->close();
    }

    std::cout << "Commit time: " << total.elapsed() << "s" << std::endl;

    return 0;
}
