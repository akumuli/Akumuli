#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_sequencer_0)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    /*
     * 9,8,7,6,5,4,3,2,1,0,10,9,8,7,6,5,4,3...
     */

    Sequencer seq(nullptr, 9);

    for (int i = 0; i < LARGE_LOOP; i++) {
        for (int j = 0; j < SMALL_LOOP; j++) {
            seq.add(TimeSeriesValue({0}, i + SMALL_LOOP - j, i));
        }
    }
}
