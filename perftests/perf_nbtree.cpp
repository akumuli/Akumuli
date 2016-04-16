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
    uint32_t caps[] = {1024*1024, 1024*1024};
    std::vector<std::string> paths = { "/tmp/volume1", "/tmp/volume2" };
    std::string metapath = "/tmp/metavol";

    Volume::create_new(paths[0].c_str(), caps[0]);
    Volume::create_new(paths[1].c_str(), caps[1]);
    MetaVolume::create_new(metapath.c_str(), 2, caps);
    auto bstore = BlockStore::open(metapath, paths);

    NBTree tree_a(42, bstore);
    NBTree tree_b(24, bstore);

    const int N = 100000000;

    Timer tm;
    for (int i = 1; i < (N+1); i++) {
        aku_Timestamp ts = i;
        double value = 0.01*i;
        if (i&1) {
            tree_a.append(ts, value);
        } else {
            tree_b.append(ts, value);
        }
        if (i % 10000 == 0) {
            bstore->flush();
        }
        if (i % 1000000 == 0) {
            std::cout << i << "\t" << tm.elapsed() << " sec" << std::endl;
            tm.restart();
        }
    }
    std::cout << "Root(A): " << tree_a.roots().at(0) << std::endl;
    std::cout << "Root(B): " << tree_b.roots().at(0) << std::endl;

    return 0;
}
