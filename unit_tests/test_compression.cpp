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
        EXPECTED_SIZE = sizeof(EXPECTED)/sizeof(uint64_t),
        BUFFER_SIZE = sizeof(buffer);
    auto 
        stream_start = buffer,
        stream_end = buffer + BUFFER_SIZE;

    Base128StreamWriter writer(stream_start, stream_end);

    // Encode
    for (int i = 0; i < EXPECTED_SIZE; i++) {
        bool result = writer.put(EXPECTED[i]);
        BOOST_REQUIRE(result);
    }
    const size_t
        USED_SIZE = writer.size();
    BOOST_REQUIRE_LT(USED_SIZE, sizeof(EXPECTED));
    BOOST_REQUIRE_GT(USED_SIZE, EXPECTED_SIZE);

    // Read it back
    uint64_t actual[EXPECTED_SIZE];
    stream_start = buffer;
    Base128StreamReader reader(stream_start, stream_end);
    for (int i = 0; i < EXPECTED_SIZE; i++) {
        actual[i] = reader.next();
    }
    BOOST_REQUIRE_EQUAL_COLLECTIONS(EXPECTED, EXPECTED + EXPECTED_SIZE, 
                                    actual, actual + EXPECTED_SIZE);
}
