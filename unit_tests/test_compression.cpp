#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>
#include <vector>

#include "compression.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_base128_1) {
    unsigned char buffer[1000];
    const uint64_t 
        EXPECTED[] = {
            0ul, 10ul, 100ul, 
            127ul, 128ul, 200ul, 
            1024ul, 10000ul, 
            1000000ul, 42ul
        };

    const size_t
        EXPECTED_SIZE = sizeof(EXPECTED)/sizeof(uint64_t);
    auto 
        stream_start = buffer,
        stream_end = buffer + EXPECTED_SIZE;

    // Encode
    for (int i = 0; i < EXPECTED_SIZE; i++) {
        Base128Int value(EXPECTED[i]);
        stream_start = value.put(stream_start, stream_end);
    }
    const size_t
        USED_SIZE = stream_start - buffer;
    BOOST_REQUIRE_LT(USED_SIZE, sizeof(EXPECTED));
    BOOST_REQUIRE_GT(USED_SIZE, EXPECTED_SIZE);

    // Read it back
    uint64_t actual[EXPECTED_SIZE];
    stream_start = buffer;
    for (int i = 0; i < EXPECTED_SIZE; i++) {
        Base128Int value;
        stream_start = value.get(stream_start, stream_end);
        actual[i] = static_cast<uint64_t>(value);
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(EXPECTED, EXPECTED + EXPECTED_SIZE, 
                                    actual, actual + EXPECTED_SIZE);
}