#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <algorithm>

#include "saxencoder.h"

using namespace Akumuli::SAX;

BOOST_AUTO_TEST_CASE(Test_sax_word) {

    std::vector<int> input = {0, 1, 7, 0x7F, 0xFFFF, 0xFFFFFFF };
    SAXWord sword(input.begin(), input.end());
    std::vector<int> output;
    sword.read_n((int)input.size(), std::back_inserter(output));
    BOOST_REQUIRE_EQUAL_COLLECTIONS(input.begin(), input.end(), output.begin(), output.end());
}


BOOST_AUTO_TEST_CASE(Test_encoding) {

    std::vector<double> input = {
        0, 1, 2, 3, 0, 1, 2, 3, 0, 3, 0, 2, 1, 3
    };

    SAXEncoder encoder(4, 4);

    std::vector<std::string> words;

    for (auto x: input) {
        std::string w;
        w.resize(4);
        if (encoder.encode(x, &w[0], w.size())) {
            words.push_back(w);
        }
    }

    std::vector<std::string> expected = {
        "abcd",
        "bcda",
        "cdab",
        "dabc",
        "abcd",
        "bcda",
        "cdad",
        "dada",
        "adac",
        "dacb",
        "acbd",
    };

    BOOST_REQUIRE_EQUAL_COLLECTIONS(words.begin(), words.end(), expected.begin(), expected.end());
}

