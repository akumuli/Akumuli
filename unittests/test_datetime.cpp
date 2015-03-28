#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "datetime.h"
#include <boost/date_time/posix_time/posix_time.hpp>


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_string_conversion) {

    const char* timestamp_str = "20060102T150405.999999999";  // RFC3339 Nano timestamp
    auto pt = boost::posix_time::from_iso_string(timestamp_str);
    aku_TimeStamp actual = DateTimeUtil::from_boost_ptime(pt);
    aku_TimeStamp expected = 1136214245999999999ul;
    BOOST_REQUIRE(actual == expected);
}
