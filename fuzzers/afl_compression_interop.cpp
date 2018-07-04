#include "storage_engine/compression.h"
#include "storage_engine/volume.h"
#include "util.h"

#include <fstream>
#include <cstdlib>
#include <algorithm>
#include <zlib.h>
#include <cstring>
#include <vector>

using namespace Akumuli;

struct UncompressedChunk {
    /** Index in `timestamps` and `paramids` arrays corresponds
      * to individual row. Each element of the `values` array corresponds to
      * specific column and row. Variable longest_row should contain
      * longest row length inside the header.
      */
    std::vector<aku_Timestamp> timestamps;
    std::vector<aku_ParamId>   paramids;
    std::vector<double>        values;
};

int main(int argc, char** argv) {
    if (argc == 1) {
        return 1;
    }
    std::string file_name(argv[1]);
    std::fstream input(file_name, std::ios::binary|std::ios::in|std::ios::out);
    UncompressedChunk header;

    double tx;
    aku_Timestamp ts;
    while(input) {
        input.read(reinterpret_cast<char*>(&ts), sizeof(ts));
        if (input) {
            input.read(reinterpret_cast<char*>(&tx), sizeof(tx));
        }
        if (input) {
            header.timestamps.push_back(ts);
            header.values.push_back(tx);
        }
    }

    for (size_t i = 1; i < header.timestamps.size(); i++) {
        if (header.timestamps.at(i) < header.timestamps.at(i-1)) {
            return -1;
        }
    }

    StorageEngine::IOVecBlock block;
    StorageEngine::IOVecBlockWriter<StorageEngine::IOVecBlock> writer(&block, 0);
    writer.init(42);
    u32 nelements = 0;
    for (u32 i = 0; i < header.timestamps.size(); i++) {
        aku_Status status = writer.put(header.timestamps.at(i), header.values.at(i));
        if (status == AKU_EOVERFLOW) {
            nelements = i;
            break;
        } else if (status != AKU_SUCCESS) {
            AKU_PANIC("Can't compress data: " + std::to_string(status));
        }
    }
    auto commit_size = writer.commit();

    ByteVector buffer(4096, 0);
    StorageEngine::DataBlockReader reader(buffer.data(), commit_size);
    // Only first `nelements` was written to `buffer`.
    for (u32 i = 0; i < nelements; i++) {
        aku_Timestamp ts;
        double tx;
        aku_Status status;
        std::tie(status, ts, tx) = reader.next();
        if (status != AKU_SUCCESS) {
            AKU_PANIC("Can't decompress data: " + std::to_string(status));
        }
        if (ts != header.timestamps.at(i)) {
            AKU_PANIC("Bad timestamp at: " + std::to_string(i));
        }
        if (tx != header.values.at(i)) {
            AKU_PANIC("Bad value at: " + std::to_string(i));
        }
    }
    return 0;
}
