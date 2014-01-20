#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_generation_move)
{
    TimeDuration td = { 1000L };
    Generation gen1(td);
    BOOST_REQUIRE(gen1.data_);
    Generation gen2(std::move(gen1));
    BOOST_REQUIRE(gen2.data_);
    BOOST_REQUIRE(!gen1.data_);
}

BOOST_AUTO_TEST_CASE(Test_generation_insert)
{
    TimeDuration td = { 1000L };
    Generation gen(td);

    for (int i = 0; i < 100; i++) {
        TimeStamp ts = { (int64_t)i };
        gen.add(ts, (ParamId)i*2, (EntryOffset)i*4);
    }

    for (int i = 0; i < 100; i++) {
        TimeStamp ts = { (int64_t)i };
        EntryOffset res[1];
        auto ret_rem = gen.find(ts, (ParamId)i*2, res, 1, 0);
        BOOST_REQUIRE(ret_rem.first == 1);
        BOOST_REQUIRE(ret_rem.second == false);
        BOOST_REQUIRE(res[0] == (EntryOffset)i*4);
    }
}
