// C++ headers
#include <iostream>
#include <vector>

// Lib headers
#include <apr.h>

// App headers
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "perftest_tools.h"

using namespace Akumuli::V2;

int main() {
    apr_initialize();

    // Create volumes
    uint32_t caps[] = {1024, 1024};
    std::vector<std::string> paths = { "/tmp/volume1", "/tmp/volume2" };
    std::string metapath = "/tmp/metavol";

    Volume::create_new(paths[0].c_str(), caps[0]);
    Volume::create_new(paths[1].c_str(), caps[1]);
    MetaVolume::create_new(metapath.c_str(), 2, caps);

    const size_t NITERS = 4096;
    std::vector<uint8_t> buffer;
    buffer.resize(4096);
    for (int i = 0; i < 4096; i++) {
        buffer[i] = rand();
    }

    // Open blockstore
    auto blockstore = BlockStore::open(metapath.c_str(), paths);

    Akumuli::PerfTimer tm;
    for (size_t ix = 0; ix < NITERS; ix++) {
        aku_Status status;
        LogicAddr addr;
        std::tie(status, addr) = blockstore->append_block(buffer.data());
        if (status != AKU_SUCCESS) {
            std::cout << "Error at " << ix << std::endl;
            return -1;
        }
    }
    std::cout << "Done writing in " << tm.elapsed() << std::endl;
    return 0;
}
