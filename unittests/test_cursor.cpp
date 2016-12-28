#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "cursor.h"
#include "akumuli_def.h"


using namespace Akumuli;


void test_cursor(int n_iter, int buf_size) {
    ConcurrentCursor cursor;
    std::vector<aku_Sample> expected;
    auto generator = [n_iter, &expected, &cursor]() {
        for (u32 i = 0u; i < (u32)n_iter; i++) {
            aku_Sample r = {};
            r.payload.float64 = i;
            r.payload.type = AKU_PAYLOAD_FLOAT;
            r.payload.size = sizeof(aku_Sample);
            cursor.put(r);
            expected.push_back(r);
        }
        cursor.complete();
    };
    std::vector<aku_Sample> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        char results[buf_size*sizeof(aku_Sample)];
        int n_read = cursor.read(results, buf_size*sizeof(aku_Sample));
        // copy with clipping
        int offset = 0;
        while(offset < n_read) {
            const aku_Sample* sample = reinterpret_cast<const aku_Sample*>(results + offset);
            actual.push_back(*sample);
            offset += std::max(sample->payload.size, (u16)sizeof(aku_Sample));
        }
    }
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());

    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).payload.float64, actual.at(i).payload.float64);
    }
}

void test_cursor_error(int n_iter, int buf_size) {
    ConcurrentCursor cursor;
    std::vector<aku_Sample> expected;
    auto generator = [n_iter, &expected, &cursor]() {
        for (u32 i = 0u; i < (u32)n_iter; i++) {
            aku_Sample r = {};
            r.payload.float64 = i;
            r.payload.type = AKU_PAYLOAD_FLOAT;
            r.payload.size = sizeof(aku_Sample);
            cursor.put(r);
            expected.push_back(r);
        }
        cursor.set_error((aku_Status)-1);
    };
    std::vector<aku_Sample> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        char results[buf_size*sizeof(aku_Sample)];
        int n_read = cursor.read(results, buf_size*sizeof(aku_Sample));
        // copy with clipping
        int offset = 0;
        while(offset < n_read) {
            const aku_Sample* sample = reinterpret_cast<const aku_Sample*>(results + offset);
            actual.push_back(*sample);
            offset += std::max(sample->payload.size, (u16)sizeof(aku_Sample));
        }
    }
    BOOST_CHECK(cursor.is_error());
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());

    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).payload.float64, actual.at(i).payload.float64);
    }
}

BOOST_AUTO_TEST_CASE(Test_cursor_0_10)
{
    test_cursor(0, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_10_10)
{
    test_cursor(10, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_10_100)
{
    test_cursor(10, 100);
}

BOOST_AUTO_TEST_CASE(Test_cursor_100_10)
{
    test_cursor(100, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_100_7)
{
    test_cursor(100, 7);
}

BOOST_AUTO_TEST_CASE(Test_cursor_error_0_10)
{
    test_cursor_error(0, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_error_10_10)
{
    test_cursor_error(10, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_error_10_100)
{
    test_cursor_error(10, 100);
}

BOOST_AUTO_TEST_CASE(Test_cursor_error_100_10)
{
    test_cursor_error(100, 10);
}

BOOST_AUTO_TEST_CASE(Test_cursor_error_100_7)
{
    test_cursor_error(100, 7);
}

