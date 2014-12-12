#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <iostream>
#include "resp.h"

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_respstream_read_integer_1) {

    const char* buffer = ":1234567890\r\n";
    MemStreamReader stream(buffer, 14);
    RESPStream resp(&stream);
    BOOST_REQUIRE_EQUAL(resp.next_type(), RESPStream::INTEGER);
    int result = -1;
    BOOST_REQUIRE(resp.read_int(&result));
    BOOST_REQUIRE_EQUAL(result, 1234567890);
}

BOOST_AUTO_TEST_CASE(Test_respstream_read_integer_2) {

    const char* buffer = "+1234567890\r\n";
    MemStreamReader stream(buffer, 14);
    RESPStream resp(&stream);
    BOOST_REQUIRE_EQUAL(resp.next_type(), RESPStream::STRING);
    int result = -1;
    BOOST_REQUIRE(!resp.read_int(&result));
    BOOST_REQUIRE_EQUAL(result, -1);
}

BOOST_AUTO_TEST_CASE(Test_respstream_read_integer_3) {

    const char* buffer = ":123fl\r\n";
    MemStreamReader stream(buffer, 14);
    RESPStream resp(&stream);
    int result = -1;
    BOOST_REQUIRE(!resp.read_int(&result));
    BOOST_REQUIRE_EQUAL(result, -1);
}

BOOST_AUTO_TEST_CASE(Test_respstream_read_integer_4) {

    const char* buffer = ":1234567890\r00";
    MemStreamReader stream(buffer, 14);
    RESPStream resp(&stream);
    int result = -1;
    BOOST_REQUIRE(!resp.read_int(&result));
    BOOST_REQUIRE_EQUAL(result, -1);
}

