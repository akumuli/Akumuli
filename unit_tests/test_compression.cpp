#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>
#include <vector>

#include "compression.h"


using namespace Akumuli;

template<class TStreamReader, class TStreamWriter>
void test_stream_read_write(TStreamReader& reader, TStreamWriter& writer) {
    const uint64_t
            EXPECTED[] = {
            0ul, 1ul, 10ul,
            67ul, 127ul, 128ul,
            1024ul, 10000ul,
            100000ul, 420000000ul
    };
    const size_t
            EXPECTED_SIZE = sizeof(EXPECTED)/sizeof(uint64_t);

    // Encode
    for (auto i = 0u; i < EXPECTED_SIZE; i++) {
        bool result = writer.put(EXPECTED[i]);
        BOOST_REQUIRE(result);
    }
    bool result = writer.close();
    BOOST_REQUIRE(result);

    const size_t
            USED_SIZE = writer.size();
    BOOST_REQUIRE_LT(USED_SIZE, sizeof(EXPECTED));
    BOOST_REQUIRE_GT(USED_SIZE, EXPECTED_SIZE);

    // Read it back
    uint64_t actual[EXPECTED_SIZE];
    for (auto i = 0u; i < EXPECTED_SIZE; i++) {
        reader.next(&actual[i]);
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(EXPECTED, EXPECTED + EXPECTED_SIZE,
            actual, actual + EXPECTED_SIZE);
}

BOOST_AUTO_TEST_CASE(Test_base128) {
    unsigned char buffer[1000];

    const size_t
            BUFFER_SIZE = sizeof(buffer);
    auto
            stream_start = buffer,
            stream_end = buffer + BUFFER_SIZE;

    Base128StreamWriter writer(stream_start, stream_end);

    Base128StreamReader reader(stream_start, stream_end);

    test_stream_read_write(reader, writer);
}

BOOST_AUTO_TEST_CASE(Test_delta) {
    unsigned char buffer[1000];

    const size_t
            BUFFER_SIZE = sizeof(buffer);
    auto
            stream_start = buffer,
            stream_end = buffer + BUFFER_SIZE;

    Base128StreamReader reader(stream_start, stream_end);
    Base128StreamWriter writer(stream_start, stream_end);
    DeltaStreamReader<Base128StreamReader, uint64_t> delta_reader(reader);
    DeltaStreamWriter<Base128StreamWriter, uint64_t> delta_writer(writer);

    test_stream_read_write(delta_reader, delta_writer);
}

BOOST_AUTO_TEST_CASE(Test_rle) {
    unsigned char buffer[1000];

    const size_t
            BUFFER_SIZE = sizeof(buffer);
    auto
            stream_start = buffer,
            stream_end = buffer + BUFFER_SIZE;

    Base128StreamReader reader(stream_start, stream_end);
    RLEStreamReader<Base128StreamReader, uint64_t> rle_reader(reader);

    Base128StreamWriter writer(stream_start, stream_end);
    RLEStreamWriter<Base128StreamWriter, uint64_t> rle_writer(writer);

    test_stream_read_write(rle_reader, rle_writer);
}

BOOST_AUTO_TEST_CASE(Test_delta_rle) {
    unsigned char buffer[1000];

    const size_t
            BUFFER_SIZE = sizeof(buffer);
    auto
            stream_start = buffer,
            stream_end = buffer + BUFFER_SIZE;

    typedef RLEStreamReader<Base128StreamReader, uint64_t> RLEStreamRdr;
    typedef RLEStreamWriter<Base128StreamWriter, uint64_t> RLEStreamWrt;
    typedef DeltaStreamReader<RLEStreamRdr, uint64_t> DeltaStreamRdr;
    typedef DeltaStreamWriter<RLEStreamWrt, uint64_t> DeltaStreamWrt;

    Base128StreamReader reader(stream_start, stream_end);
    Base128StreamWriter writer(stream_start, stream_end);
    RLEStreamRdr rle_reader(reader);
    RLEStreamWrt rle_writer(writer);
    DeltaStreamRdr delta_reader(rle_reader);
    DeltaStreamWrt delta_writer(rle_writer);

    test_stream_read_write(delta_reader, delta_writer);
}

