#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"

#include "cpp-btree/btree_map.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_generation_move)
{
    TimeDuration td = { 1000L };
    Generation gen1(td, 100);
    TimeStamp ts = { 111L };
    gen1.add(ts, 0, 0);
    BOOST_REQUIRE(!gen1.data_.empty());
    Generation gen2(std::move(gen1));
    BOOST_REQUIRE(!gen2.data_.empty());
    BOOST_REQUIRE(gen1.data_.empty());
}

BOOST_AUTO_TEST_CASE(Test_generation_insert)
{
    TimeDuration td = { 1000L };
    Generation gen(td, 1000);

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

BOOST_AUTO_TEST_CASE(Test_generation_insert_overflow_by_size)
{
    TimeDuration td = { 1000L };
    Generation gen(td, 4);

    TimeStamp ts = { (int64_t)0 };
    BOOST_REQUIRE(gen.add(ts, (ParamId)1, (EntryOffset)1) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts, (ParamId)2, (EntryOffset)2) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts, (ParamId)3, (EntryOffset)3) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts, (ParamId)4, (EntryOffset)4) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts, (ParamId)5, (EntryOffset)5) == AKU_WRITE_STATUS_OVERFLOW);
    BOOST_REQUIRE(gen.add(ts, (ParamId)6, (EntryOffset)6) == AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(Test_generation_insert_overflow_by_time)
{
    TimeDuration td = { 8L };
    Generation gen(td, 1000);

    TimeStamp ts0 = { (int64_t)0 };
    TimeStamp ts1 = { (int64_t)1 };
    TimeStamp ts3 = { (int64_t)3 };
    TimeStamp ts9 = { (int64_t)9 };
    BOOST_REQUIRE(gen.add(ts0, (ParamId)1, (EntryOffset)1) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts1, (ParamId)2, (EntryOffset)2) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts3, (ParamId)3, (EntryOffset)3) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(gen.add(ts9, (ParamId)4, (EntryOffset)4) == AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(Test_generation_find)
{
    TimeDuration td = { 1000L };
    Generation gen(td, 1000);

    TimeStamp ts = { 0L };
    ParamId id = 1;
    for (int i = 0; i < 100; i++) {
        gen.add(ts, id, (EntryOffset)i*4);
    }

    size_t seek = 0;
    for (int i = 0; i < 100; i++) {
        EntryOffset res[1];
        auto ret_rem = gen.find(ts, id, res, 1, seek);
        seek += ret_rem.first;
        BOOST_REQUIRE(res[0] == (EntryOffset)i*4);
    }
}

BOOST_AUTO_TEST_CASE(Test_generation_oldest) {
    TimeDuration td = { 1000L };
    Generation gen(td, 1000);

    TimeStamp oldest;
    auto res = gen.get_oldest_timestamp(&oldest);
    BOOST_REQUIRE(res == false);

    ParamId id = 1;
    for (int i = 0; i < 100; i++) {
        TimeStamp ts = { 100L - i };
        gen.add(ts, id, (EntryOffset)i*4);
    }
    res = gen.get_oldest_timestamp(&oldest);
    BOOST_REQUIRE(res == true);
    BOOST_REQUIRE(oldest.value == 1L);
}


BOOST_AUTO_TEST_CASE(Test_cache_add) {
    //Cache cache;
    //cache.
}
