#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "cursor.h"
#include "page.h"


using namespace Akumuli;


void test_cursor(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<aku_Sample> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (uint32_t i = 0u; i < (uint32_t)n_iter; i++) {
            aku_Sample r;
            r.payload.value.blob.begin = reinterpret_cast<void*>(i);
            r.payload.value.blob.size = sizeof(i);
            r.payload.type = aku_PData::BLOB;
            cursor.put(caller, r);
            expected.push_back(r);
        }
        cursor.complete(caller);
    };
    std::vector<aku_Sample> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        aku_Sample results[buf_size];
        int n_read = cursor.read(results, buf_size);
        std::copy(results, results + n_read, std::back_inserter(actual));
    }
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());

    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).payload.value.blob.begin, actual.at(i).payload.value.blob.begin);
    }
}

void test_cursor_error(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<aku_Sample> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (uint32_t i = 0u; i < (uint32_t)n_iter; i++) {
            aku_Sample r;
            r.payload.value.blob.begin = reinterpret_cast<void*>(i);
            cursor.put(caller, r);
            expected.push_back(r);
        }
        cursor.set_error(caller, -1);
    };
    std::vector<aku_Sample> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        aku_Sample results[buf_size];
        int n_read = cursor.read(results, buf_size);
        std::copy(results, results + n_read, std::back_inserter(actual));
    }
    BOOST_CHECK(cursor.is_error());
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());

    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).payload.value.blob.begin, actual.at(i).payload.value.blob.begin);
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

struct SortPred {
    uint32_t dir;
    bool operator () (int64_t lhs, int64_t rhs) {
        if (dir == AKU_CURSOR_DIR_FORWARD) {
            return lhs < rhs;
        } else if (dir == AKU_CURSOR_DIR_BACKWARD) {
            return lhs > rhs;
        }
        BOOST_FAIL("Bad direction");
        return false;
    }

    template<class It>
    void check_order(It begin, It end) {
        int64_t prev;
        for(auto i = begin; i != end; i++) {
            if (i != begin) {
                if (dir == AKU_CURSOR_DIR_FORWARD) {
                    if (*i < prev) {
                        BOOST_REQUIRE(*i >= prev);
                    }
                } else if (dir == AKU_CURSOR_DIR_BACKWARD) {
                    if (*i > prev) {
                        BOOST_REQUIRE(*i <= prev);
                    }
                } else {
                    BOOST_FAIL("Bad direction");
                }
            }
            prev = *i;
        }
    }
};

struct PageWrapper {
    char* buf;
    PageHeader* page;
    uint32_t page_id;
    size_t count;
    std::vector<int64_t> timestamps;

    PageWrapper(int page_size, uint32_t id) {
        count = 0;
        page_id = id;
        buf = new char[page_size];
        page = new (buf) PageHeader(0, page_size, (int)page_id);
        init();
    }

    ~PageWrapper() {
        delete[] buf;
    }

    void init() {
        aku_Timestamp ts = rand() % 1000;
        while(true) {
            aku_MemRange load = {(void*)&page_id, sizeof(page_id)};
            auto status = page->add_entry(ts, rand(), load);
            timestamps.push_back(ts);
            ts += rand() % 100;
            if (status != AKU_SUCCESS) {
                break;
            }
            count++;
        }
        SortPred pred = { AKU_CURSOR_DIR_FORWARD };
        pred.check_order(timestamps.begin(), timestamps.end());
    }
};

static const int COROUTINE_LIMIT = 10000;

static void coroutine(Caller& caller, InternalCursor* cursor, bool backward) {
    if (!backward) {
        int next = 0;
        for (int i = 0; i < COROUTINE_LIMIT; i++) {
            aku_Sample sample;
            sample.paramid = next & 3;
            sample.timestamp = next;
            sample.payload.type = aku_PData::FLOAT;
            sample.payload.value.float64 = 0.0;
            cursor->put(caller, sample);
            next += std::rand() % 100;
        }
    } else {
        int next = std::numeric_limits<int>::max();
        for (int i = 0; i < COROUTINE_LIMIT; i++) {
            aku_Sample sample;
            sample.paramid = next & 3;
            sample.timestamp = next;
            sample.payload.type = aku_PData::FLOAT;
            sample.payload.value.float64 = 0.0;
            cursor->put(caller, sample);
            next -= std::rand() % 100;
        }

    }
    cursor->complete(caller);
}

template<class FanInCursor>
void test_fan_in_cursor(uint32_t dir, int n_cursors, int page_size) {

    std::vector<CoroCursor> cursors(n_cursors);
    for (int i = 0; i < n_cursors; i++) {
        CoroCursor* cursor = &cursors[i];
        cursor->start(std::bind(&coroutine, std::placeholders::_1, cursor, dir == AKU_CURSOR_DIR_BACKWARD));
    }

    std::vector<ExternalCursor*> ecur;
    std::transform(cursors.begin(), cursors.end(),
                   std::back_inserter(ecur),
                   [](Cursor& c) { return &c; });

    FanInCursor cursor(&ecur[0], n_cursors, (int)dir);

    aku_Sample results[0x100];
    int count = 0;
    std::vector<int64_t> actual_results;  // must be sorted
    std::vector<int64_t> expected_results;
    while(!cursor.is_done()) {
        int n_read = cursor.read(results, 0x100);
        count += n_read;
        for (int i = 0; i < n_read; i++) {
            actual_results.push_back(results[i].timestamp);
            expected_results.push_back(results[i].timestamp);
        }
    }
    cursor.close();

    SortPred s = {dir};
    std::sort(expected_results.begin(), expected_results.end(), s);

    s.check_order(actual_results.begin(), actual_results.end());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual_results.begin(), actual_results.end(), expected_results.begin(), expected_results.end());
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_1_f)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 1, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_2_f)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 10, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_3_f)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 1, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_4_f)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 10, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_1_b)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 1, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_2_b)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 10, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_3_b)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 1, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_stackless_fan_in_cursor_4_b)
{
    test_fan_in_cursor<StacklessFanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 10, 100000 + sizeof(PageHeader));
}

