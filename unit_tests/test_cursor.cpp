#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>
#include <vector>

#include "cursor.h"
#include "page.h"


using namespace Akumuli;


void test_cursor(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<CursorResult> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (EntryOffset i = 0u; i < (EntryOffset)n_iter; i++) {
            cursor.put(caller, i, nullptr);
            expected.push_back(std::make_pair(i, (PageHeader*)nullptr));
        }
        cursor.complete(caller);
    };
    std::vector<CursorResult> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        CursorResult results[buf_size];
        int n_read = cursor.read(results, buf_size);
        std::copy(results, results + n_read, std::back_inserter(actual));
    }
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).first, actual.at(i).first);
    }
}

void test_cursor_error(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<CursorResult> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (EntryOffset i = 0u; i < (EntryOffset)n_iter; i++) {
            cursor.put(caller, i, nullptr);
            expected.push_back(std::make_pair(i, (PageHeader*)nullptr));
        }
        cursor.set_error(caller, -1);
    };
    std::vector<CursorResult> actual;
    cursor.start(generator);
    while(!cursor.is_done()) {
        CursorResult results[buf_size];
        int n_read = cursor.read(results, buf_size);
        std::copy(results, results + n_read, std::back_inserter(actual));
    }
    BOOST_CHECK(cursor.is_error());
    cursor.close();

    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    for(size_t i = 0; i < actual.size(); i++) {
        BOOST_REQUIRE_EQUAL(expected.at(i).first, actual.at(i).first);
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

struct PageWrapper {
    char* buf;
    PageHeader* page;
    uint32_t page_id;
    std::vector<EntryOffset> offsets;
    size_t count;

    PageWrapper(int page_size, uint32_t id) {
        count = 0;
        page_id = id;
        buf = new char[page_size];
        page = new (buf) PageHeader(Index, 0, page_size, (int)page_id);
        init();
    }

    ~PageWrapper() {
        delete buf;
    }

    void init() {
        const auto esize = Entry::get_size(sizeof(int));
        char ebuf[esize];
        while(true) {
            Entry* entry = new (ebuf) Entry(esize);
            entry->param_id = rand() % 100;
            entry->time.value = rand();
            entry->value[0] = page_id;
            auto status = page->add_entry(*entry);
            if (status != AKU_SUCCESS) {
                return;
            }
            count++;
            auto offset = page->last_offset;
            offsets.push_back(offset);
        }
        page->sort();
    }
};

void test_fan_in_cursor(uint32_t dir, int n_cursors, int page_size) {
    std::vector<PageWrapper> pages;
    pages.reserve(n_cursors);
    for (int i = 0; i < n_cursors; i++) {
        pages.emplace_back(page_size, (uint32_t)i);
    }

    auto match_all = [](ParamId) { return SearchQuery::MATCH; };
    SearchQuery q(match_all, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, dir);

    std::vector<CoroCursor> cursors(n_cursors);
    for (int i = 0; i < n_cursors; i++) {
        PageHeader* page = pages[i].page;
        CoroCursor* cursor = &cursors[i];
        cursor->start(std::bind(&PageHeader::search, page, std::placeholders::_1, cursor, q));
    }

    std::vector<ExternalCursor*> ecur;
    std::transform(cursors.begin(), cursors.end(),
                   std::back_inserter(ecur),
                   [](Cursor& c) { return &c; });

    auto cursor = FanInCursor::start(&ecur[0], n_cursors, (int)dir);
    CursorResult results[0x100];
    int count = 0;
    std::vector<CursorResult> actual_results;
    while(!cursor->is_done()) {
        int n_read = cursor->read(results, 0x100);
        count += n_read;
        for (int i = 0; i < n_read; i++) {
            actual_results.push_back(results[i]);
        }
    }
    cursor->close();
}

// TODO: check merge correctness

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_0)
{
    test_fan_in_cursor(AKU_CURSOR_DIR_BACKWARD, 10, 100000);
}
