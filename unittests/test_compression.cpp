#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "storage_engine/compression.h"


using namespace Akumuli;

static const uint64_t
    EXPECTED[] = {
    0ul, 1ul, 10ul,
    67ul, 127ul, 128ul,
    1024ul, 10000ul,
    100000ul, 420000000ul
};

static const size_t
        EXPECTED_SIZE = sizeof(EXPECTED)/sizeof(uint64_t);

template<class TStreamWriter>
void test_stream_write(TStreamWriter& writer) {
    // Encode
    for (auto i = 0u; i < EXPECTED_SIZE; i++) {
        writer.put(EXPECTED[i]);
    }
    writer.commit();

    const size_t
            USED_SIZE = writer.size();
    BOOST_REQUIRE_LT(USED_SIZE, sizeof(EXPECTED));
    BOOST_REQUIRE_GT(USED_SIZE, EXPECTED_SIZE);
}

template<class TVal, class TStreamWriter, class TStreamReader>
void test_stream_chunked_op(TStreamWriter& writer, TStreamReader& reader, size_t nsteps, bool sort_input=false) {
    std::vector<TVal> input = {0};
    const size_t step_size = 16;
    const size_t input_size = step_size*nsteps;
    TVal value = 100000;

    // Generate
    for (int i = 0; i < (input_size-1); i++) {
        int delta = TVal(rand() % 1000 - 500);
        value += delta;
        input.push_back(value);
    }

    if (sort_input) {
        std::sort(input.begin(), input.end());
    }

    // Encode
    for (auto offset = 0u; offset < input_size; offset += step_size) {
        auto success = writer.tput(input.data() + offset, step_size);
        if (!success) {
            BOOST_REQUIRE(success);
        }
    }

    // Decode and compare results
    std::vector<TVal> results;
    for (auto offset = 0ul; offset < input_size; offset++) {
        auto next = reader.next();
        results.push_back(next);
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(input.begin(), input.end(),
                                    results.begin(), results.end());
}

template<class TStreamReader>
void test_stream_read(TStreamReader& reader) {
    // Read it back
    uint64_t actual[EXPECTED_SIZE];
    for (auto i = 0u; i < EXPECTED_SIZE; i++) {
        actual[i] = reader.next();
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(EXPECTED, EXPECTED + EXPECTED_SIZE,
            actual, actual + EXPECTED_SIZE);
}

template<>
void test_stream_read(Base128StreamReader& reader) {
    // Read it back
    uint64_t actual[EXPECTED_SIZE];
    for (auto i = 0u; i < EXPECTED_SIZE; i++) {
        actual[i] = reader.next<uint64_t>();
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(EXPECTED, EXPECTED + EXPECTED_SIZE,
            actual, actual + EXPECTED_SIZE);
}

    
BOOST_AUTO_TEST_CASE(Test_base128) {

    std::vector<unsigned char> data;
    data.resize(1000);

    Base128StreamWriter writer(data.data(), data.data() + data.size());
    test_stream_write(writer);

    Base128StreamReader reader(data.data(), data.data() + data.size());
    test_stream_read(reader);
}


BOOST_AUTO_TEST_CASE(Test_delta_rle) {

    std::vector<unsigned char> data;
    data.resize(1000);

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    DeltaStreamWriter<RLEStreamWriter<uint64_t>, uint64_t> delta_writer(wstream);
    test_stream_write(delta_writer);

    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaStreamReader<RLEStreamReader<uint64_t>, uint64_t> delta_reader(rstream);
    test_stream_read(delta_reader);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_rle_vbyte_0) {
    std::vector<unsigned char> data;
    data.resize(10*1024);  // 10KB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    DeltaStreamWriter<RLEStreamWriter<uint64_t>, uint64_t> delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaStreamReader<RLEStreamReader<uint64_t>, uint64_t> delta_reader(rstream);

    test_stream_chunked_op<uint64_t>(delta_writer, delta_reader, 100, true);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_rle_vbyte_1) {
    std::vector<unsigned char> data;
    data.resize(1*1024*1024);  // 1MB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    DeltaStreamWriter<RLEStreamWriter<uint64_t>, uint64_t> delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaStreamReader<RLEStreamReader<uint64_t>, uint64_t> delta_reader(rstream);

    test_stream_chunked_op<uint64_t>(delta_writer, delta_reader, 10000, true);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_rle_zigzag_vbyte_0) {
    std::vector<unsigned char> data;
    data.resize(10*1024);  // 10KB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    ZDeltaRLEWriter delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    ZDeltaRLEReader delta_reader(rstream);

    test_stream_chunked_op<int64_t>(delta_writer, delta_reader, 100, false);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_rle_zigzag_vbyte_1) {
    std::vector<unsigned char> data;
    data.resize(1*1024*1024);  // 1MB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    ZDeltaRLEWriter delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    ZDeltaRLEReader delta_reader(rstream);

    test_stream_chunked_op<int64_t>(delta_writer, delta_reader, 10000, false);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_delta_vbyte_0) {
    std::vector<unsigned char> data;
    data.resize(4*1024);  // 4KB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    DeltaDeltaStreamWriter<16, uint64_t> delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaDeltaStreamReader<16, uint64_t> delta_reader(rstream);

    test_stream_chunked_op<uint64_t>(delta_writer, delta_reader, 100, true);
}

BOOST_AUTO_TEST_CASE(Test_chunked_delta_delta_vbyte_1) {
    std::vector<unsigned char> data;
    data.resize(1*1024*1024);  // 1MB of storage

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    DeltaDeltaStreamWriter<16, uint64_t> delta_writer(wstream);
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaDeltaStreamReader<16, uint64_t> delta_reader(rstream);

    test_stream_chunked_op<uint64_t>(delta_writer, delta_reader, 10000, true);
}


BOOST_AUTO_TEST_CASE(Test_rle) {
    std::vector<unsigned char> data;
    data.resize(1000);

    Base128StreamWriter wstream(data.data(), data.data() + data.size());
    RLEStreamWriter<uint64_t> rle_writer(wstream);

    test_stream_write(rle_writer);

    Base128StreamReader rstream(data.data(), data.data() + data.size());
    RLEStreamReader<uint64_t> rle_reader(rstream);
    test_stream_read(rle_reader);
}

BOOST_AUTO_TEST_CASE(Test_bad_offset_decoding)
{
    // this replicates real problem //
    std::vector<uint32_t> actual;
    const uint32_t BASE_OFFSET = 3221191859u;
    const uint32_t OFFSET_STEP = 8u;
    uint32_t current = BASE_OFFSET;
    for(int i = 0; i < 10000; i++) {
        actual.push_back(current);
        current -= OFFSET_STEP;
    }

    ByteVector data;
    data.resize(100000);
    Base128StreamWriter bstream(data.data(), data.data() + data.size());

    DeltaRLEWriter wstream(bstream);
    for (auto off: actual) {
        wstream.put(off);
    }
    wstream.commit();

    std::vector<uint32_t> expected;
    Base128StreamReader rstream(data.data(), data.data() + data.size());
    DeltaRLEReader rlestream(rstream);
    for (int i = 0; i < 10000; i++) {
        expected.push_back((uint32_t)rlestream.next());
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
}

void test_doubles_compression(std::vector<double> input) {
    ByteVector buffer;
    buffer.resize(input.size()*10);
    Base128StreamWriter wstream(buffer.data(), buffer.data() + buffer.size());
    size_t nblocks = CompressionUtil::compress_doubles(input, wstream);
    std::vector<double> output;
    output.resize(input.size());
    Base128StreamReader rstream(buffer.data(), buffer.data() + buffer.size());
    CompressionUtil::decompress_doubles(rstream, nblocks, &output);

    BOOST_REQUIRE_EQUAL(input.size(), output.size());
    for(auto i = 0u; i < input.size(); i++) {
        auto actual = input.at(i);
        auto expected = output.at(i);
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_1_series) {
    std::vector<double> input = {
        100.1001,
        100.0999,
        100.0998,
        100.0997,
        100.0996,
    };
    test_doubles_compression(input);
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_2_series) {
    std::vector<double> input = {
        100.1001,
        200.4999,
        100.0999,
        200.499,
        100.0998,
        200.49,
        100.0997,
        200.5,
        100.0996,
        200.5001,
    };
    test_doubles_compression(input);
}

//! Generate time-series from random walk
struct RandomWalk {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>    distribution;
    double                              value;

    RandomWalk(double start, double mean, double stddev)
        : generator(randdev())
        , distribution(mean, stddev)
        , value(start)
    {
    }

    double generate() {
        value += distribution(generator);
        return value;
    }
};


void test_float_compression(double start) {
    RandomWalk rwalk(start, 1., .11);
    int N = 10000;
    std::vector<double> samples;
    std::vector<uint8_t> block;
    block.resize(N*9, 0);

    // Compress
    Base128StreamWriter wstream(block.data(), block.data() + block.size());
    FcmStreamWriter writer(wstream);
    for (int ix = 0; ix < N; ix++) {
        double val = rwalk.generate();
        writer.put(val);
        samples.push_back(val);
    }
    writer.commit();

    // Decompress
    Base128StreamReader rstream(block.data(), block.data() + block.size());
    FcmStreamReader reader(rstream);
    for (int ix = 0; ix < N; ix++) {
        double val = reader.next();
        if (val != samples.at(ix)) {
            BOOST_REQUIRE(val == samples.at(ix));
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_float_compression_0) {
    test_float_compression(0);
}

BOOST_AUTO_TEST_CASE(Test_float_compression_1) {
    test_float_compression(1E-100);
}

BOOST_AUTO_TEST_CASE(Test_float_compression_2) {
    test_float_compression(1E100);
}

BOOST_AUTO_TEST_CASE(Test_float_compression_3) {
    test_float_compression(-1E-100);
}

BOOST_AUTO_TEST_CASE(Test_float_compression_4) {
    test_float_compression(-1E100);
}

void test_block_compression(double start) {
    RandomWalk rwalk(start, 1., .11);
    int N = 10000;
    std::vector<aku_Timestamp> timestamps;
    std::vector<double> values;
    std::vector<uint8_t> block;
    block.resize(4096);

    aku_Timestamp ts = rand();
    for (int i = 0; i < N; i++) {
        values.push_back(rwalk.generate());
        int skew = rand() % 100;
        ts += skew;
        timestamps.push_back(ts);
    }

    // compress

    SeriesSlice slice;
    slice.id = 42;
    slice.offset = 0;
    slice.ts = timestamps.data();
    slice.value = values.data();
    slice.size = N;

    auto status = CompressionUtil::encode_block(&slice, block.data(), block.size());
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_NE(slice.offset, 0);

    // decompress

    std::vector<aku_Timestamp> out_timestamps;
    std::vector<double> out_values;

    // gen number of elements stored in block
    uint32_t nelem = CompressionUtil::number_of_elements_in_block(block.data(), block.size());
    BOOST_REQUIRE_NE(nelem, 0);
    out_timestamps.resize(nelem);
    out_values.resize(nelem);

    SeriesSlice out_slice;
    out_slice.id = 0;
    out_slice.offset = 0;
    out_slice.size = nelem;
    out_slice.ts = out_timestamps.data();
    out_slice.value = out_values.data();

    status = CompressionUtil::decode_block(block.data(), block.size(), &out_slice);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_NE(out_slice.offset, 0);
    BOOST_REQUIRE_EQUAL(out_slice.offset, nelem);
    BOOST_REQUIRE_EQUAL(out_slice.id, 42);

    for (size_t i = 0; i < nelem; i++) {
        if (timestamps.at(i) != out_timestamps.at(i)) {
            BOOST_FAIL("Bad timestamp at " << i << ", expected: " << timestamps.at(i) <<
                       ", actual: " << out_timestamps.at(i));
        }
        if (values.at(i) != out_values.at(i)) {
            BOOST_FAIL("Bad value at " << i << ", expected: " << values.at(i) <<
                       ", actual: " << out_values.at(i));
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_block_compression_0) {
    test_block_compression(0);
}

BOOST_AUTO_TEST_CASE(Test_block_compression_1) {
    test_block_compression(1E-100);
}

BOOST_AUTO_TEST_CASE(Test_block_compression_2) {
    test_block_compression(1E100);
}

BOOST_AUTO_TEST_CASE(Test_block_compression_3) {
    test_block_compression(-1E-100);
}

BOOST_AUTO_TEST_CASE(Test_block_compression_4) {
    test_block_compression(-1E100);
}

void test_chunk_header_compression(double start) {

    UncompressedChunk expected;

    const int NROWS = 10000;  // number of rows in one series
    const int NSER = 2;  // number of series
    RandomWalk rwalk(start, 1., .11);

    // Fill chunk header
    for (int i = 0; i < NROWS; i++) {
        expected.paramids.push_back(0);
    }
    for (int i = 0; i < NROWS; i++) {
        expected.paramids.push_back(1);
    }

    for (int i = 0; i < NSER; i++) {
        for (int j = 0; j < NROWS; j++) {
            expected.timestamps.push_back(j);
        }
    }

    expected.values.resize(NROWS*NSER);
    for (int row = 0; row < NROWS*NSER; row++) {
        double cell = rwalk.generate();
        expected.values.at(row) = cell;
    }

    aku_Timestamp tsbegin = 0, tsend = 0;
    uint32_t cardinality = 0;

    struct Writer : ChunkWriter {
        std::vector<unsigned char> buffer;

        Writer(size_t size) {
            buffer.resize(size);
        }

        virtual aku_MemRange allocate() {
            return { (void*)buffer.data(), (uint32_t)buffer.size() };
        }

        virtual aku_Status commit(size_t bytes_written) {
            if (bytes_written > buffer.size()) {
                return AKU_EOVERFLOW;
            }
            buffer.resize(bytes_written);
            return AKU_SUCCESS;
        }
    };

    // Original chunk size
    size_t total_bytes = 0;
    for (auto i = 0u; i < expected.paramids.size(); i++) {
        total_bytes += sizeof(aku_ParamId) +
                       sizeof(aku_Timestamp) +
                       sizeof(double);
    }

    Writer writer(total_bytes);

    auto status = CompressionUtil::encode_chunk(&cardinality, &tsbegin, &tsend, &writer, expected);
    BOOST_REQUIRE(status == AKU_SUCCESS);

    // Calculate compression ratio
    size_t compressed_bytes = writer.buffer.size();
    double compression_ratio = double(total_bytes)/double(compressed_bytes);
    BOOST_REQUIRE(compression_ratio > 1.0);

    UncompressedChunk actual;
    const unsigned char* pbegin = writer.buffer.data();
    const unsigned char* pend = writer.buffer.data() + writer.buffer.size();
    status = CompressionUtil::decode_chunk(&actual, pbegin, pend, cardinality);
    BOOST_REQUIRE(status == AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.paramids.begin(), expected.paramids.end(),
                                    actual.paramids.begin(), actual.paramids.end());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.timestamps.begin(), expected.timestamps.end(),
                                    actual.timestamps.begin(), actual.timestamps.end());
    for (int i = 0; i < NROWS*NSER; i++) {
        BOOST_REQUIRE_EQUAL(expected.values.at(i), actual.values.at(i));
    }
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression_0) {
    test_chunk_header_compression(0);
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression_1) {
    test_chunk_header_compression(1E-100);
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression_2) {
    test_chunk_header_compression(1E100);
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression_3) {
    test_chunk_header_compression(-1E-100);
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression_4) {
    test_chunk_header_compression(-1E100);
}
