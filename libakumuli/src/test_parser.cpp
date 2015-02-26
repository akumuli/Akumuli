#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "seriesparser.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_parser_0) {

    SeriesParser::to_normal_form(0, 0, 0, 0, 0);
}
