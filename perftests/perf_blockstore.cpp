// C++ headers
#include <iostream>
#include <vector>

// Lib headers
#include <apr.h>
#include <sys/time.h>

// App headers
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"


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
    u32 caps[] = {1024*1024, 1024*1024};
    std::vector<std::string> paths = { "/tmp/volume1", "/tmp/volume2" };
    std::string metapath = "/tmp/metavol";

    Volume::create_new(paths[0].c_str(), caps[0]);
    Volume::create_new(paths[1].c_str(), caps[1]);
    MetaVolume::create_new(metapath.c_str(), 2, caps);

    const size_t NITERS = 4096*1024;
    std::shared_ptr<Block> buffer = std::make_shared<Block>();
    for (int i = 0; i < 4096; i++) {
        buffer->get_data()[i] = static_cast<u8>(rand());
    }

    // Open blockstore
    auto blockstore = FixedSizeFileStorage::open(metapath.c_str(), paths);

    Timer tm;
    double prev_time = tm.elapsed();
    for (size_t ix = 0; ix < NITERS; ix++) {
        aku_Status status;
        LogicAddr addr;
        std::tie(status, addr) = blockstore->append_block(buffer);
        if (status != AKU_SUCCESS) {
            std::cout << "Error at " << ix << std::endl;
            return -1;
        }
        if ((ix & 0xFF) == 0) {
            // 1Mb was written
            double current_time = tm.elapsed();
            double seconds = current_time - prev_time;
            double mbs = 1.0/seconds;
            printf("%g MB/sec\r", mbs);
            prev_time = tm.elapsed();
        }
        if ((ix & 0xFFF) == 0) {
            blockstore->flush();
        }
        if (ix % (1024*1024) == 0) {
            std::cout << "Next volume, done at " << tm.elapsed() << "sec" << std::endl;
        }
    }
    std::cout << "Done writing in " << tm.elapsed() << std::endl;
    return 0;
}
