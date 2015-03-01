#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "seriesparser.h"

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_seriesparser_0) {

    const char* series = " cpu  region=europe   host=127.0.0.1 ";
    auto len = strlen(series);
    char out[40];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    std::string expected = "cpu host=127.0.0.1 region=europe";
    std::string actual = std::string(out);
    BOOST_REQUIRE_EQUAL(expected, actual);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_1) {

    const char* series = "cpu";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_2) {

    const char* series = "cpu region host=127.0.0.1 ";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_3) {

    const char* series = "cpu region=europe host";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_4) {

    const char* series = "bs";
    auto len = AKU_LIMITS_MAX_SNAME + 1;
    char out[10];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_5) {

    const char* series = "bs";
    auto len = AKU_LIMITS_MAX_SNAME - 1;
    char out[10];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + 10, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_ARG);
}
