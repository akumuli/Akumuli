#include "akumuli.h"
#include "storage_engine/blockstore.h"
#include "storage_engine/compression.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"

using namespace Akumuli;
using namespace StorageEngine;

int main(int argc, char** argv) {
    if (argc == 1) {
        return 1;
    }
    std::string file_name(argv[1]);
    std::fstream input(file_name, std::ios::binary|std::ios::in|std::ios::out);

    std::vector<aku_Timestamp> ts;
    std::vector<double> xs;
    int pivot = 0;
    bool recover = false;
    if (input) {
        input.read(reinterpret_cast<char*>(pivot), sizeof(int));
    }
    while(input) {
        aku_Timestamp t;
        double x;
        input.read(reinterpret_cast<char*>(&t), sizeof(t));
        if (input) {
            input.read(reinterpret_cast<char*>(&x), sizeof(x));
        }
        if (input) {
            ts.push_back(t);
            xs.push_back(x);
        }
    }
    if (pivot < 0) {
        pivot = -1*pivot;
        recover = true;
    }
    if (ts.size() != xs.size()) {
        return -1;
    }
    for (size_t i = 1; i < header.timestamps.size(); i++) {
        if (header.timestamps.at(i) < header.timestamps.at(i-1)) {
            return -1;
        }
    }
    if (ts.size() < static_cast<size_t>(pivot)) {
        return -1;
    }
    // TODO: use limited memstore
    auto memstore = BlockStoreBuilder::create_memstore();
    std::vector<LogicAddr> addr;
    auto nbtree = std::make_shared<NBTreeExtentsList>(42, addr, memstore);

    for (int i = 0; i < pivot; i++) {
        bool need2fush = nbtree->append(ts.at(i), xs.at(i));
        if (need2flush) {
            addr = nbtree->get_roots();
        }
    }

    if (!recover) {
        // normal operation
        addr = nbtree->close();
    }

    // reopen
    nbtree.reset(new NBTreeExtentsList(id, addr, memstore));
    nbtree->force_init();

    // add remaining elements
    for (int i = pivot; i < static_cast<int>(ts.size()); i++) {
        bool need2fush = nbtree->append(ts.at(i), xs.at(i));
        if (need2flush) {
            addr = nbtree->get_roots();
        }
    }

    // TODO: read data back and check results
}
