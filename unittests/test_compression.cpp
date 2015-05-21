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

void test_doubles_compression(std::vector<double> input) {
    ByteVector buffer;
    size_t nblocks = CompressionUtil::compress_doubles(input,  &buffer);
    std::vector<double> output;
    CompressionUtil::decompress_doubles(buffer, nblocks, &output);

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
    test_doubles_compression(input);
}

BOOST_AUTO_TEST_CASE(Test_doubles_compression_2_series) {
    std::vector<double> input = {
        100.1001, 200.4999,
        100.0999, 200.499,
        100.0998, 200.49,
        100.0997, 200.5,
        100.0996, 200.5001
    };
    test_doubles_compression(input);
}
