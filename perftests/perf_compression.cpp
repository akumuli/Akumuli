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

//! Generate time-series from random walk
struct RandomWalk {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>    distribution;
    size_t                              N;
    std::vector<double>                 values;

    RandomWalk(double start, double mean, double stddev, size_t N)
        : generator(randdev())
        , distribution(mean, stddev)
        , N(N)
    {
        values.resize(N, start);
    }

    double generate(aku_ParamId id) {
        values.at(id) += 0.01;// distribution(generator);
        return sin(values.at(id));
    }

    void add_anomaly(aku_ParamId id, double value) {
        values.at(id) += value;
    }
};

UncompressedChunk read_data(fs::path path) {
    UncompressedChunk res;
    std::fstream in(path.c_str());
    std::string line;
    aku_ParamId base_pid = 1;
    std::map<std::string, aku_ParamId> pid_map;
    while(std::getline(in, line)) {
        std::istringstream lstr(line);
        std::string series, timestamp, value;
        std::getline(lstr, series, ',');
        std::getline(lstr, timestamp, ',');
        std::getline(lstr, value, ',');

        aku_ParamId id;
        auto pid_it = pid_map.find(series);
        if (pid_it == pid_map.end()) {
            pid_map[series] = base_pid;
            id = base_pid;
            base_pid++;
        } else {
            id = pid_it->second;
        }
        res.paramids.push_back(id);

        res.timestamps.push_back(DateTimeUtil::from_iso_string(timestamp.c_str()));

        res.values.push_back(std::stod(value));

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

    const size_t UNCOMPRESSED_SIZE = header.paramids.size()*8    // Didn't count lengths and offsets
                                   + header.timestamps.size()*8  // because because this arrays contains
                                   + header.values.size()*8;     // no information and should be compressed
                                                                 // to a few bytes

    auto bstore = StorageEngine::BlockStoreBuilder::create_memstore();
    auto cstore = std::make_shared<StorageEngine::ColumnStore>(bstore);

    aku_ParamId previd = 0;
    std::vector<u64> rpoints;
    for (size_t i = 0; i < header.paramids.size(); i++) {
        aku_Sample sample = {};
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.paramid = header.paramids[i];
        sample.timestamp = header.timestamps[i];
        sample.payload.float64 = header.values[i];
        if (previd != sample.paramid) {
            cstore->create_new_column(sample.paramid);
        }
        cstore->write(sample, &rpoints, nullptr);
    }
    auto store_stats = bstore->get_stats();
    auto uncommitted = cstore->_get_uncommitted_memory();
    cstore->close();
    //std::cout << "Block store: " << store_stats.nblocks <<
    //             " blocks used, uncommitted size: " << uncommitted << std::endl;

    // Compress using zlib

    const size_t COMPRESSED_SIZE = store_stats.nblocks*store_stats.block_size + uncommitted;
    const float BYTES_PER_EL = float(COMPRESSED_SIZE)/header.paramids.size();
    const float COMPRESSION_RATIO = float(UNCOMPRESSED_SIZE)/COMPRESSED_SIZE;

    // Save compression stats

    runresults.uncompressed         = UNCOMPRESSED_SIZE;
    runresults.compressed           = COMPRESSED_SIZE;
    runresults.nelements            = header.timestamps.size();
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
