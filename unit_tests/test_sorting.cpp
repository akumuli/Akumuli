#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "sort.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_insertion_sort_0)
{
    std::vector<int> actual = {
        0, 10, 22, 3, 2, 9, 14
    };

    insertion_sort(actual.begin(), actual.end(), std::less<int>());

    std::vector<int> expected = {
        0, 2, 3, 9, 10, 14, 22
    };

    for (int i = 0; i < (int)actual.size(); i++) {
        BOOST_CHECK_EQUAL(expected[i], actual[i]);
    }
}
