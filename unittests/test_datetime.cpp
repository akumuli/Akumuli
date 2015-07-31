#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "datetime.h"
#include <boost/date_time/posix_time/posix_time.hpp>


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_string_iso_to_timestamp_conversion) {

    const char* timestamp_str = "20060102T150405.999999999";  // ISO timestamp
    aku_Timestamp actual = DateTimeUtil::from_iso_string(timestamp_str);
    aku_Timestamp expected = 1136214245999999999ul;
    BOOST_REQUIRE(actual == expected);

    // To string
    const int buffer_size = 100;
    char buffer[buffer_size];
    int len = DateTimeUtil::to_iso_string(actual, buffer, buffer_size);

    BOOST_REQUIRE_EQUAL(len, 26);

    BOOST_REQUIRE_EQUAL(std::string(buffer), std::string(timestamp_str));

}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_seconds) {

    const char* test_case = "10s";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 3u);
    aku_Duration expected = 10000000000ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_nanos) {

    const char* test_case = "111n";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 4u);
    aku_Duration expected = 111ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_nanos2) {

    const char* test_case = "111";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 3u);
    aku_Duration expected = 111ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_us) {

    const char* test_case = "111us";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 5u);
    aku_Duration expected = 111000ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_ms) {

    const char* test_case = "111ms";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 5u);
    aku_Duration expected = 111000000ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}

BOOST_AUTO_TEST_CASE(Test_string_to_duration_minutes) {

    const char* test_case = "111m";
    aku_Duration actual = DateTimeUtil::parse_duration(test_case, 4u);
    aku_Duration expected = 111*60*1000000000ul;
    BOOST_REQUIRE_EQUAL(actual, expected);
}
