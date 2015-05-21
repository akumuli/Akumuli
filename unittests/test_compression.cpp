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
    writer.close();

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

    
BOOST_AUTO_TEST_CASE(Test_base128) {
    std::vector<unsigned char> data;
    data.resize(1000);
    Base128StreamWriter writer(data.data(), data.data() + data.size());
    test_stream_write(writer);

    Base128StreamReader<uint64_t> reader(data.data(), data.data() + data.size());
    test_stream_read(reader);
}

BOOST_AUTO_TEST_CASE(Test_delta) {
    std::vector<unsigned char> data;
    data.resize(1000);
    Base128StreamWriter stream(data.data(), data.data() + data.size());
    DeltaStreamWriter<Base128StreamWriter, uint64_t> delta_writer(stream);
    test_stream_write(delta_writer);

    DeltaStreamReader<Base128StreamReader<uint64_t>, uint64_t> delta_reader(data.data(), data.data() + data.size());
    test_stream_read(delta_reader);
}

BOOST_AUTO_TEST_CASE(Test_rle) {
    std::vector<unsigned char> data;
    data.resize(1000);

    Base128StreamWriter stream(data.data(), data.data() + data.size());
    RLEStreamWriter<uint64_t> rle_writer(stream);

    test_stream_write(rle_writer);

    RLEStreamReader<Base128StreamReader<uint64_t>, uint64_t> rle_reader(data.begin(), data.end());
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
    DeltaRLEWriter wstream(data);
    for (auto off: actual) {
        wstream.put(off);
    }
    wstream.close();

    std::vector<uint32_t> expected;
    DeltaRLEReader rstream(data.data(), data.data() + data.size());
    for (int i = 0; i < 10000; i++) {
        expected.push_back((uint32_t)rstream.next());
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
}

void test_doubles_compression(std::vector<double> input, std::vector<aku_ParamId> params) {
    ByteVector buffer;
    size_t nblocks = CompressionUtil::compress_doubles(input, params, &buffer);
    std::vector<double> output;
    CompressionUtil::decompress_doubles(buffer, nblocks, params, &output);

    BOOST_REQUIRE_EQUAL_COLLECTIONS(input.begin(), input.end(), output.begin(), output.end());
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_1_series) {
    std::vector<double> input = {
        100.1001,
        100.0999,
        100.0998,
        100.0997,
        100.0996
    };
    std::vector<aku_ParamId> params = {
        0, 0, 0, 0, 0
    };
    test_doubles_compression(input, params);
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_2_series) {
    std::vector<double> input = {
        100.1001, 200.4999,
        100.0999, 200.499,
        100.0998, 200.49,
        100.0997, 200.5,
        100.0996, 200.5001
    };
    std::vector<aku_ParamId> params = {
        0, 1, 0, 1, 0, 1, 0, 1, 0, 1
    };
    test_doubles_compression(input, params);
}
