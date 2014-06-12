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

    Sequencer seq(nullptr, {10});

}
