#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <iostream>

#include "saxencoder.h"

using namespace Akumuli::SAX;

BOOST_AUTO_TEST_CASE(Test_sax_word) {

    std::vector<int> input = {0, 1, 2, 3, 4, 5, 6, 7, 0x7F, 0xFFFF, 0xFFFFFFF };
    SAXWord sword(input.begin(), input.end());
    std::vector<int> output;
    sword.read_n((int)input.size(), std::back_inserter(output));
    BOOST_REQUIRE_EQUAL_COLLECTIONS(input.begin(), input.end(), output.begin(), output.end());
}

