#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <algorithm>

#include "invertedindex.h"

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_inverted_index_0) {
    InvertedIndex index(1024);

    for (int i = 0; i < 1000; i++) {
        char buffer[128];
        int n = snprintf(buffer, 128, "%d", i);
        index.append(i, buffer, buffer + n);
    }

    for (int i = 0; i < 1000; i++) {
        char buffer[128];
        int n = snprintf(buffer, 128, "%d", i);
        auto results = index.get_count(buffer, buffer + n);
        // shouldn't be any conflicts with this setup
        BOOST_REQUIRE_EQUAL(results.size(), 1u);
        BOOST_REQUIRE_EQUAL(results.at(0).first, i);
        BOOST_REQUIRE_EQUAL(results.at(0).second, 1);
    }
}
