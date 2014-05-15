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

    PageWrapper(int page_size, uint32_t id) {
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

    std::vector<PageHeader*> headers;
    std::transform(pages.begin(), pages.end(),
                   std::back_inserter(headers),
                   [](PageWrapper& pw) {
                        return pw.page;
                   });

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

    FanInCursor cursor(&ecur[0], &headers[0], n_cursors, (int)dir);
    EntryOffset offsets[0x100];
    int count = 0;
    int iter = 0;
    while(!cursor.is_done()) {
        iter++;
        if (iter == 140) {
            BOOST_MESSAGE("Ololo");
        }
        int n_read = cursor.read(offsets, 0x100);
        count += n_read;
    }
    cursor.close();
}

// TODO: check merge correctness

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_0)
{
    test_fan_in_cursor(AKU_CURSOR_DIR_BACKWARD, 10, 100000);
}
