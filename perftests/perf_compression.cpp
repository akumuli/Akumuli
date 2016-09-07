#include "storage_engine/compression.h"
#include "perftest_tools.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

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

int main(int argc, char** argv) {
    const u64 TEST_SIZE = 100000;
    UncompressedChunk header;
    std::cout << "Testing timestamp sequence" << std::endl;
    int c = 100;
    std::vector<aku_ParamId> ids;
    RandomWalk rwalk(10.0, 0.0, 0.01, 1);
    for (u64 ts = 0; ts < TEST_SIZE; ts++) {
        int k = rand() % 2;
        if (k) {
            c++;
        } else if (c > 0) {
            c--;
        }
        header.timestamps.push_back((ts + c) << 8);
        header.values.push_back(rwalk.generate(0));
    }

    ByteVector out;
    out.resize(TEST_SIZE*24);
    //out.resize(4096);

    const size_t UNCOMPRESSED_SIZE = header.timestamps.size()*8
                                   + header.values.size()*8;

    const size_t nruns = 1000;
    size_t total_bytes = 0;
    std::vector<double> timings(nruns, .0);
    for (size_t k = 0; k < nruns; k++) {
        PerfTimer tm;
        Akumuli::StorageEngine::DataBlockWriter writer(42, out.data(), static_cast<int>(out.size()));
        for (size_t i = 0; i < header.timestamps.size(); i++) {
            writer.put(header.timestamps[i], header.values[i]);
        }
        size_t outsize = writer.commit();
        timings.at(k) = tm.elapsed();
        total_bytes += outsize;
    }

    std::cout << "\nAkumuli" << std::endl;
    std::cout << "Completed at " << std::accumulate(timings.begin(), timings.end(), .0, std::plus<double>()) << std::endl;
    std::cout << "Fastest run: " << std::accumulate(timings.begin(), timings.end(), 1E10, [](double a, double b) {
        return std::min(a, b);
    }) << std::endl;
    std::cout << "Total bytes: " << total_bytes << std::endl;
    std::cout << "Compression: " << (double(UNCOMPRESSED_SIZE)/double(total_bytes/nruns)) << std::endl;
    std::cout << "Bytes/point: " << (double(total_bytes/nruns)/TEST_SIZE) << std::endl;

    // Test zstd compression
    total_bytes = 0;
    for (size_t k = 0; k < nruns; k++) {
        PerfTimer tm;
        auto ctx = ZSTD_createCStream();
        ZSTD_initCStream(ctx, 1);
        u64 tss[128];
        ZSTD_outBuffer outbuf = {};
        outbuf.dst = out.data();
        outbuf.size = out.size();
        for (size_t ix = 0; ix < header.timestamps.size(); ix++) {
            tss[ix % 64] = header.timestamps[ix];
            tss[ix % 64 + 64] = *reinterpret_cast<double const*>(&header.values[ix]);
            if (ix % 64 == 0 && ix) {
                ZSTD_inBuffer inbuf = {};
                inbuf.src = &tss;
                inbuf.size = 128*8;
                auto hint = ZSTD_compressStream(ctx, &outbuf, &inbuf);
                while (inbuf.size != inbuf.pos) {
                    hint = ZSTD_compressStream(ctx, &outbuf, &inbuf);
                    if (ZSTD_isError(hint)) {
                        std::cout << ZSTD_getErrorName(hint) << std::endl;
                        abort();
                    }
                }
                if (ZSTD_isError(hint)) {
                    std::cout << ZSTD_getErrorName(hint) << std::endl;
                    abort();
                }
                ZSTD_flushStream(ctx, &outbuf);
            }
        }
        size_t outsize = ZSTD_endStream(ctx, &outbuf);
        if (ZSTD_isError(outsize)) {
            std::cout << ZSTD_getErrorName(outsize) << std::endl;
            abort();
        }
        ZSTD_freeCStream(ctx);
        timings.at(k) = tm.elapsed();
        total_bytes += outbuf.pos;
    }
    std::cout << "\nZstandard" << std::endl;
    std::cout << "Completed at " << std::accumulate(timings.begin(), timings.end(), .0, std::plus<double>()) << std::endl;
    std::cout << "Fastest run: " << std::accumulate(timings.begin(), timings.end(), 1E10, [](double a, double b) {
        return std::min(a, b);
    }) << std::endl;
    std::cout << "Total bytes: " << total_bytes << std::endl;
    std::cout << "Compression: " << (double(UNCOMPRESSED_SIZE)/double(total_bytes/nruns)) << std::endl;
    std::cout << "Bytes/point: " << (double(total_bytes/nruns)/TEST_SIZE) << std::endl;
}
