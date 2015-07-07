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
