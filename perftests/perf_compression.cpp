#include "compression.h"
#include "perftest_tools.h"

#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <zlib.h>
#include <cstring>

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
        values.at(id) += distribution(generator);
        return values.at(id);
    }

    void add_anomaly(aku_ParamId id, double value) {
        values.at(id) += value;
    }
};

int main() {
    const uint64_t N_TIMESTAMPS = 1000;
    const uint64_t N_PARAMS = 100;
    UncompressedChunk header;
    std::cout << "Testing timestamp sequence" << std::endl;
    int c = 100;
    std::vector<aku_ParamId> ids;
    for (uint64_t id = 0; id < N_PARAMS; id++) { ids.push_back(id); }
    RandomWalk rwalk(10.0, 0.0, 0.01, N_PARAMS);
    for (uint64_t id = 0; id < N_PARAMS; id++) {
        for (uint64_t ts = 0; ts < N_TIMESTAMPS; ts++) {
            header.paramids.push_back(ids[id]);
            int k = rand() % 2;
            if (k) {
                c++;
            } else if (c > 0) {
                c--;
            }
            header.timestamps.push_back((ts + c) << 8);
            header.values.push_back(rwalk.generate(0));
        }
    }

    ByteVector out;
    out.resize(N_PARAMS*N_TIMESTAMPS*24);

    const size_t UNCOMPRESSED_SIZE = header.paramids.size()*8    // Didn't count lengths and offsets
                                   + header.timestamps.size()*8  // because because this arrays contains
                                   + header.values.size()*8;     // no information and should be compressed
                                                                 // to a few bytes

    struct Writer : ChunkWriter {
        ByteVector *out;
        Writer(ByteVector *out) : out(out) {}

        virtual aku_MemRange allocate() {
            aku_MemRange range = {
                out->data(),
                static_cast<uint32_t>(out->size())
            };
            return range;
        }

        //! Commit changes
        virtual aku_Status commit(size_t bytes_written) {
            out->resize(bytes_written);
            return AKU_SUCCESS;
        }
    };
    Writer writer(&out);

    aku_Timestamp tsbegin, tsend;
    uint32_t n;
    auto status = CompressionUtil::encode_chunk(&n, &tsbegin, &tsend, &writer, header);
    if (status != AKU_SUCCESS) {
        std::cout << "Encoding error" << std::endl;
        return 1;
    }

    // Compress using zlib

    // Ids copy (zlib need all input data to be aligned because it uses SSE2 internally)
    Bytef* pgz_ids = (Bytef*)aligned_alloc(64, header.paramids.size()*8);
    memcpy(pgz_ids, header.paramids.data(), header.paramids.size()*8);
    // Timestamps copy
    Bytef* pgz_ts = (Bytef*)aligned_alloc(64, header.timestamps.size()*8);
    memcpy(pgz_ts, header.timestamps.data(), header.timestamps.size()*8);
    // Values copy
    Bytef* pgz_val = (Bytef*)aligned_alloc(64, header.values.size()*8);
    memcpy(pgz_val, header.values.data(), header.values.size()*8);

    const auto gz_max_size = N_PARAMS*N_TIMESTAMPS*24;
    Bytef* pgzout = (Bytef*)aligned_alloc(64, gz_max_size);
    uLongf gzoutlen = gz_max_size;
    size_t total_gz_size = 0, id_gz_size = 0, ts_gz_size = 0, float_gz_size = 0;
    // compress param ids
    auto zstatus = compress(pgzout, &gzoutlen, pgz_ids, header.paramids.size()*8);
    if (zstatus != Z_OK) {
        std::cout << "GZip error" << std::endl;
        exit(zstatus);
    }
    total_gz_size += gzoutlen;
    id_gz_size = gzoutlen;
    gzoutlen = gz_max_size;
    // compress timestamps
    zstatus = compress(pgzout, &gzoutlen, pgz_ts, header.timestamps.size()*8);
    if (zstatus != Z_OK) {
        std::cout << "GZip error" << std::endl;
        exit(zstatus);
    }
    total_gz_size += gzoutlen;
    ts_gz_size = gzoutlen;
    gzoutlen = gz_max_size;
    // compress floats
    zstatus = compress(pgzout, &gzoutlen, pgz_val, header.values.size()*8);
    if (zstatus != Z_OK) {
        std::cout << "GZip error" << std::endl;
        exit(zstatus);
    }
    total_gz_size += gzoutlen;
    float_gz_size = gzoutlen;

    const float GZ_BPE = float(total_gz_size)/header.paramids.size();
    const float GZ_RATIO = float(UNCOMPRESSED_SIZE)/float(total_gz_size);


    const size_t COMPRESSED_SIZE = out.size();
    const float BYTES_PER_EL = float(COMPRESSED_SIZE)/header.paramids.size();
    const float COMPRESSION_RATIO = float(UNCOMPRESSED_SIZE)/COMPRESSED_SIZE;

    std::cout << "Uncompressed: " << UNCOMPRESSED_SIZE       << std::endl
              << "  compressed: " << COMPRESSED_SIZE         << std::endl
              << "    elements: " << header.paramids.size()  << std::endl
              << "  bytes/elem: " << BYTES_PER_EL            << std::endl
              << "       ratio: " << COMPRESSION_RATIO       << std::endl
    ;

    std::cout << "Gzip stats: " << std::endl
              << "bytes/elem: " << GZ_BPE << std::endl
              << "     ratio: " << GZ_RATIO << std::endl
              << "  id bytes: " << id_gz_size << std::endl
              << "  ts bytes: " << ts_gz_size << std::endl
              << " val bytes: " << float_gz_size << std::endl;


    // Try to decompress
    UncompressedChunk decomp;
    const unsigned char* pbegin = out.data();
    const unsigned char* pend = pbegin + out.size();
    CompressionUtil::decode_chunk(&decomp, pbegin, pend, header.timestamps.size());
    bool first_error = true;
    for (auto i = 0u; i < header.timestamps.size(); i++) {
        if (header.timestamps.at(i) != decomp.timestamps.at(i) && first_error) {
            std::cout << "Error, bad timestamp at " << i << std::endl;
            first_error = false;
        }
        if (header.paramids.at(i) != decomp.paramids.at(i) && first_error) {
            std::cout << "Error, bad paramid at " << i << std::endl;
            first_error = false;
        }
        double origvalue = header.values.at(i);
        double decvalue = decomp.values.at(i);
        if (origvalue != decvalue && first_error) {
            std::cout << "Error, bad value at " << i << std::endl;
            std::cout << "Expected: " << origvalue << std::endl;
            std::cout << "Actual:   " << decvalue << std::endl;
            first_error = false;
        }
    }

    // Bench compression process
    const int NRUNS = 1000;
    PerfTimer tm;
    aku_Status tstatus;
    volatile uint32_t vn;
    ByteVector vec;
    for (int i = 0; i < NRUNS; i++) {
        vec.resize(N_PARAMS*N_TIMESTAMPS*24);
        Writer w(&vec);
        aku_Timestamp ts;
        uint32_t n;
        tstatus = CompressionUtil::encode_chunk(&n, &ts, &ts, &w, header);
        if (tstatus != AKU_SUCCESS) {
            std::cout << "Encoding error" << std::endl;
            return 1;
        }
        vn = n;
    }
    double elapsed = tm.elapsed();
    std::cout << "Elapsed: " << elapsed << " " << vn << std::endl;
}
