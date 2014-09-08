#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
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
    Base128StreamWriter<uint64_t> writer(data);
    test_stream_write(writer);

    Base128StreamReader<uint64_t, ByteVector::const_iterator> reader(data.begin(), data.end());
    test_stream_read(reader);
}

BOOST_AUTO_TEST_CASE(Test_delta) {
    std::vector<unsigned char> data;
    DeltaStreamWriter<Base128StreamWriter<uint64_t>, uint64_t> delta_writer(data);
    test_stream_write(delta_writer);

    DeltaStreamReader<Base128StreamReader<uint64_t, ByteVector::const_iterator>, uint64_t> delta_reader(data.begin(), data.end());
    test_stream_read(delta_reader);
}

BOOST_AUTO_TEST_CASE(Test_rle) {
    std::vector<unsigned char> data;

    RLEStreamWriter<Base128StreamWriter<uint64_t>, uint64_t> rle_writer(data);
    test_stream_write(rle_writer);

    RLEStreamReader<Base128StreamReader<uint64_t, ByteVector::const_iterator>, uint64_t> rle_reader(data.begin(), data.end());
    test_stream_read(rle_reader);
}

BOOST_AUTO_TEST_CASE(Test_delta_rle) {
    typedef RLEStreamReader<Base128StreamReader<uint64_t, ByteVector::const_iterator>, uint64_t> RLEStreamRdr;
    typedef RLEStreamWriter<Base128StreamWriter<uint64_t>, uint64_t> RLEStreamWrt;
    typedef DeltaStreamReader<RLEStreamRdr, uint64_t> DeltaStreamRdr;
    typedef DeltaStreamWriter<RLEStreamWrt, uint64_t> DeltaStreamWrt;

    std::vector<unsigned char> data;
    DeltaStreamWrt delta_writer(data);
    test_stream_write(delta_writer);

    DeltaStreamRdr delta_reader(data.begin(), data.end());
    test_stream_read(delta_reader);
}

