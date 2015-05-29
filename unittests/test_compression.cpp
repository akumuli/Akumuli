#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "compression.h"


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

void test_doubles_compression(std::vector<ChunkValue> input) {
    ByteVector buffer;
    buffer.resize(input.size()*10);
    Base128StreamWriter wstream(buffer.data(), buffer.data() + buffer.size());
    size_t nblocks = CompressionUtil::compress_doubles(input, wstream);
    std::vector<ChunkValue> output;
    output.resize(input.size());
    for (auto& item: output) {
        item.type = ChunkValue::FLOAT;
    }
    Base128StreamReader rstream(buffer.data(), buffer.data() + buffer.size());
    CompressionUtil::decompress_doubles(rstream, nblocks, &output);

    BOOST_REQUIRE_EQUAL(input.size(), output.size());
    for(auto i = 0u; i < input.size(); i++) {
        auto actual = input.at(i);
        auto expected = output.at(i);
        BOOST_REQUIRE_EQUAL(actual.type, expected.type);
        BOOST_REQUIRE_EQUAL(actual.value.floatval, expected.value.floatval);
    }
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_1_series) {
    std::vector<ChunkValue> input = {
        { ChunkValue::FLOAT, 100.1001 },
        { ChunkValue::FLOAT, 100.0999 },
        { ChunkValue::FLOAT, 100.0998 },
        { ChunkValue::FLOAT, 100.0997 },
        { ChunkValue::FLOAT, 100.0996 },
    };
    test_doubles_compression(input);
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_2_series) {
    std::vector<ChunkValue> input = {
        { ChunkValue::FLOAT, 100.1001},
        { ChunkValue::FLOAT, 200.4999},
        { ChunkValue::FLOAT, 100.0999},
        { ChunkValue::FLOAT, 200.499},
        { ChunkValue::FLOAT, 100.0998},
        { ChunkValue::FLOAT, 200.49},
        { ChunkValue::FLOAT, 100.0997},
        { ChunkValue::FLOAT, 200.5},
        { ChunkValue::FLOAT, 100.0996},
        { ChunkValue::FLOAT, 200.5001},
    };
    test_doubles_compression(input);
}

//! Generate time-series from random walk
struct RandomWalk {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>    distribution;
    double                              value;

    RandomWalk(double mean, double stddev)
        : generator(randdev())
        , distribution(mean, stddev)
        , value(0)
    {
    }

    double generate() {
        value += distribution(generator);
        return value;
    }
};

void test_chunk_header_compression() {

    ChunkHeader expected;

    const int NROWS = 10000;  // number of rows in one series
    const int NSER = 2;  // number of series
    RandomWalk rwalk(1, .11);

    // Fill chunk header
    // 1 - double
    // 2 - blob
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
        ChunkValue cell;
        if (row < NROWS) {
            cell.type = ChunkValue::FLOAT;
            cell.value.floatval = rwalk.generate();
            expected.values.at(row) = cell;
        } else {
            cell.type = ChunkValue::BLOB;
            cell.value.blobval.length = 100;
            cell.value.blobval.offset = row;
            expected.values.at(row) = cell;
        }
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
        total_bytes += sizeof(aku_ParamId) + sizeof(aku_Timestamp) + sizeof(int) +
                       sizeof(double); // blob length+offset if of the same size
    }

    Writer writer(total_bytes*2);

    auto status = CompressionUtil::encode_chunk(&cardinality, &tsbegin, &tsend, &writer, expected);
    BOOST_REQUIRE(status == AKU_SUCCESS);

    // Calculate compression ratio
    size_t compressed_bytes = writer.buffer.size();
    double compression_ratio = double(total_bytes)/double(compressed_bytes);
    BOOST_REQUIRE(compression_ratio > 1.0);

    ChunkHeader actual;
    const unsigned char* pbegin = writer.buffer.data();
    const unsigned char* pend = writer.buffer.data() + writer.buffer.size();
    CompressionUtil::decode_chunk(&actual, &pbegin, pend, 0, 0, 0);

    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.paramids.begin(), expected.paramids.end(),
                                    actual.paramids.begin(), actual.paramids.end());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.timestamps.begin(), expected.timestamps.end(),
                                    actual.timestamps.begin(), actual.timestamps.end());
    for (int i = 0; i < NROWS*NSER; i++) {
        BOOST_REQUIRE_EQUAL(expected.values.at(i).type, actual.values.at(i).type);
        if (expected.values.at(i).type == ChunkValue::FLOAT) {
            BOOST_REQUIRE_EQUAL(expected.values.at(i).value.floatval,
                                actual.values.at(i).value.floatval);
        } else {
            BOOST_REQUIRE_EQUAL(expected.values.at(i).value.blobval.length,
                                actual.values.at(i).value.blobval.length);
            BOOST_REQUIRE_EQUAL(expected.values.at(i).value.blobval.offset,
                                actual.values.at(i).value.blobval.offset);
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_chunk_compression) {
    test_chunk_header_compression();
}
