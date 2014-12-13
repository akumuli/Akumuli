#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <iostream>
#include "stream.h"

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_stream_1) {

    std::string expected = "hello world";
    MemStreamReader stream_reader(expected.data(), expected.size());
    Byte buffer[1024] = {};
    BOOST_REQUIRE(!stream_reader.is_eof());
    auto bytes_read = stream_reader.read(buffer, 1024);
    BOOST_REQUIRE_EQUAL(static_cast<size_t>(bytes_read), expected.size());
    BOOST_REQUIRE_EQUAL(std::string(buffer), expected);
    BOOST_REQUIRE(stream_reader.is_eof());
}

BOOST_AUTO_TEST_CASE(Test_stream_2) {

    std::string expected = "hello world";
    MemStreamReader stream_reader(expected.data(), expected.size());
    Byte buffer[1024] = {};
    stream_reader.close();
    auto bytes_read = stream_reader.read(buffer, 1024);
    BOOST_REQUIRE_EQUAL(static_cast<size_t>(bytes_read), 0);
}

BOOST_AUTO_TEST_CASE(Test_stream_3) {

    std::string expected = "abcde";
    MemStreamReader stream_reader(expected.data(), expected.size());
    BOOST_REQUIRE_EQUAL(stream_reader.pick(), 'a');
    BOOST_REQUIRE_EQUAL(stream_reader.get(),  'a');
    BOOST_REQUIRE_EQUAL(stream_reader.get(),  'b');
    BOOST_REQUIRE_EQUAL(stream_reader.get(),  'c');
    BOOST_REQUIRE_EQUAL(stream_reader.pick(), 'd');
    BOOST_REQUIRE_EQUAL(stream_reader.get(),  'd');
}
