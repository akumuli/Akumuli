#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>
#include <vector>

#include "cursor.h"


using namespace Akumuli;


void test_cursor(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<EntryOffset> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (EntryOffset i = 0u; i < (EntryOffset)n_iter; i++) {
            cursor.put(caller, i);
            expected.push_back(i);
        }
        cursor.complete(caller);
    };
    std::vector<EntryOffset> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        EntryOffset offsets[buf_size];
        int n_read = cursor.read(offsets, buf_size);
        std::copy(offsets, offsets + n_read, std::back_inserter(actual));
    }
    cursor.close();
    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.begin(), expected.end(), actual.begin(), actual.end());
}

void test_cursor_error(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<EntryOffset> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (EntryOffset i = 0u; i < (EntryOffset)n_iter; i++) {
            cursor.put(caller, i);
            expected.push_back(i);
        }
        cursor.set_error(caller, -1);
    };
    std::vector<EntryOffset> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        EntryOffset offsets[buf_size];
        int n_read = cursor.read(offsets, buf_size);
        std::copy(offsets, offsets + n_read, std::back_inserter(actual));
    }
    BOOST_CHECK(cursor.is_error());
    cursor.close();
    BOOST_REQUIRE_EQUAL_COLLECTIONS(expected.begin(), expected.end(), actual.begin(), actual.end());
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
