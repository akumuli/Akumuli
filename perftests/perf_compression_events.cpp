#include "storage_engine/column_store.h"
#include "perftest_tools.h"
#include "datetime.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <algorithm>
#include <zlib.h>
#include <cstring>
#include <map>

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

using namespace Akumuli;

struct UncompressedChunk {
    size_t total_size_bytes;
    std::vector<std::string> values;
};

UncompressedChunk read_data(fs::path path) {
    UncompressedChunk res = {};
    std::fstream in(path.c_str());
    std::string line;
    while(std::getline(in, line)) {
        if (line.empty()) continue;
        res.total_size_bytes += line.size();
        res.values.push_back(line);
    }
    return res;
}

struct TestRunResults {
    // Akumuli stats
    std::string file_name;
    size_t uncompressed;
    size_t compressed;
    size_t nelements;

    double bytes_per_element;
    double compression_ratio;

    // Gzip stats
    double gz_bytes_per_element;
    double gz_compression_ratio;
    double gz_compressed;

    // Performance
    std::vector<double> perf;
    std::vector<double> gz_perf;
};

TestRunResults run_tests(fs::path path) {
    TestRunResults runresults;
    runresults.file_name = fs::basename(path);

    auto header = read_data(path);

    const size_t UNCOMPRESSED_SIZE = header.total_size_bytes
                                   + header.values.size()*16;

    auto bstore = StorageEngine::BlockStoreBuilder::create_memstore();
    auto cstore = std::make_shared<StorageEngine::ColumnStore>(bstore);

    std::vector<u64> rpoints;
    std::vector<char> buffer;
    buffer.resize(5000);
    aku_ParamId paramid = 10101;
    cstore->create_new_column(paramid);
    for (size_t i = 0; i < header.values.size(); i++) {
        auto line = header.values.at(i);
        auto payload_size = std::min(4000, static_cast<int>(line.size()));
        aku_Sample* sample = reinterpret_cast<aku_Sample*>(buffer.data());
        sample->payload.type = AKU_PAYLOAD_EVENT;
        sample->payload.size = payload_size + sizeof(aku_Sample);
        sample->paramid = paramid;
        sample->timestamp = 1000000*(i + 1);
        memcpy(sample->payload.data, line.data(), payload_size);
        auto res = cstore->write(*sample, &rpoints, nullptr);
        if (res != Akumuli::StorageEngine::NBTreeAppendResult::OK &&
            res != Akumuli::StorageEngine::NBTreeAppendResult::OK_FLUSH_NEEDED) {
            std::cout << "Can't write data" << std::endl;
            std::terminate();
        }
    }
    auto store_stats = bstore->get_stats();
    auto uncommitted = cstore->_get_uncommitted_memory();
    cstore->close();
    //std::cout << "Block store: " << store_stats.nblocks <<
    //             " blocks used, uncommitted size: " << uncommitted << std::endl;

    // Compress using zlib

    const size_t COMPRESSED_SIZE = store_stats.nblocks*store_stats.block_size + uncommitted;
    const float BYTES_PER_EL = float(COMPRESSED_SIZE)/header.values.size();
    const float COMPRESSION_RATIO = float(UNCOMPRESSED_SIZE)/COMPRESSED_SIZE;

    // Save compression stats

    runresults.uncompressed         = UNCOMPRESSED_SIZE;
    runresults.compressed           = COMPRESSED_SIZE;
    runresults.nelements            = header.values.size();
    runresults.bytes_per_element    = BYTES_PER_EL;
    runresults.compression_ratio    = COMPRESSION_RATIO;

    // Try to decompress
    // TBD
    return runresults;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Path to dataset required" << std::endl;
        exit(1);
    }

    // Iter directory
    fs::path dir{argv[1]};
    fs::directory_iterator begin(dir), end;
    std::vector<fs::path> files;
    std::list<TestRunResults> results;
    for (auto it = begin; it != end; it++) {
        if (it->path().extension() == ".csv") {
            files.push_back(*it);
        }
    }
    std::sort(files.begin(), files.end());
    for (auto fname: files) {
        //std::cout << "Run tests for " << fs::basename(fname) << std::endl;
        results.push_back(run_tests(fname));
    }

    // Write table
    std::cout << "| File name | num elements | uncompressed | compressed | ratio | bytes/el |" << std::endl;
    std::cout << "| ----- | ---- | ----- | ---- | ----- | ---- | " << std::endl;
    for (auto const& run: results) {
        std::cout << run.file_name << " | " <<
                     run.nelements << " | " <<
                     run.uncompressed << " | " <<
                     run.compressed << " | " <<
                     run.compression_ratio << " | " <<
                     run.bytes_per_element << " | " <<
                     std::endl;
    }

}
