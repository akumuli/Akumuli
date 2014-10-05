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

BOOST_AUTO_TEST_CASE(Test_bad_offset_decoding)
{
    // copy from page.cpp //
    typedef Base128StreamWriter<int64_t> __Base128OffWriter;                    // int64_t is used instead of uint32_t
    typedef RLEStreamWriter<__Base128OffWriter, int64_t> __RLEOffWriter;        // for a reason. Numbers is not always
    typedef ZigZagStreamWriter<__RLEOffWriter, int64_t> __ZigZagOffWriter;      // increasing here so we can get negatives
    typedef DeltaStreamWriter<__ZigZagOffWriter, int64_t> DeltaRLEOffWriter;    // after delta encoding (ZigZag coding

    // Base128 -> RLE -> ZigZag -> Delta -> Offset
    typedef Base128StreamReader<uint64_t, const unsigned char*> __Base128OffReader;
    typedef RLEStreamReader<__Base128OffReader, int64_t> __RLEOffReader;
    typedef ZigZagStreamReader<__RLEOffReader, int64_t> __ZigZagOffReader;
    typedef DeltaStreamReader<__ZigZagOffReader, int64_t> DeltaRLEOffReader;

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
    DeltaRLEOffWriter wstream(data);
    for (auto off: actual) {
        wstream.put(off);
    }
    wstream.close();

    std::vector<uint32_t> expected;
    DeltaRLEOffReader rstream(data.data(), data.data() + data.size());
    for (int i = 0; i < 10000; i++) {
        expected.push_back((uint32_t)rstream.next());
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
}

