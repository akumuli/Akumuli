#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "akumuli_def.h"
#include "page.h"

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(TestPaging1)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    BOOST_CHECK_EQUAL(0, page->get_entries_count());
}

BOOST_AUTO_TEST_CASE(TestPaging2)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    auto free_space_before = page->get_free_space();
    char buffer[128];
    auto entry = new (buffer) Entry(128);
    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto free_space_after = page->get_free_space();
    BOOST_CHECK_EQUAL(free_space_before - free_space_after, 128 + sizeof(EntryOffset));
}

BOOST_AUTO_TEST_CASE(TestPaging3)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[4096];
    auto entry = new (buffer) Entry(4096);
    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(TestPaging4)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[128];
    auto entry = new (buffer) Entry(1);
    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_BAD_DATA);
}

BOOST_AUTO_TEST_CASE(TestPaging5)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[222];
    auto entry = new (buffer) Entry(222);
    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto len = page->get_entry_length(0);
    BOOST_CHECK_EQUAL(len, 222);
}

BOOST_AUTO_TEST_CASE(TestPaging6)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[64];
    TimeStamp inst = {1111L};
    auto entry = new (buffer) Entry(3333, inst, 64);
    for (int i = 0; i < 10; i++) {
        entry->value[i] = i + 1;
    }

    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);

    entry->param_id = 0;
    for (int i = 0; i < 10; i++) {
        entry->value[i] = i + 1;
    }
    TimeStamp inst2 = {1111L};
    entry->time = inst2;

    int len = page->copy_entry(0, entry);
    BOOST_CHECK_EQUAL(len, 64);
    BOOST_CHECK_EQUAL(entry->length, 64);
    BOOST_CHECK_EQUAL(entry->param_id, 3333);
}

BOOST_AUTO_TEST_CASE(TestPaging7)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[64];
    TimeStamp inst = {1111L};
    auto entry = new (buffer) Entry(3333, inst, 64);
    for (int i = 0; i < 10; i++) {
        entry->value[i] = i + 1;
    }
    auto result = page->add_entry(*entry);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);

    auto centry = page->read_entry(0);
    BOOST_CHECK_EQUAL(centry->length, 64);
    BOOST_CHECK_EQUAL(centry->param_id, 3333);
}

BOOST_AUTO_TEST_CASE(TestPaging8)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, 4096, 0);
    char buffer[64];
    TimeStamp inst = {1111L};

    auto entry1 = new (buffer) Entry(1, inst, 64);
    page->add_entry(*entry1);

    auto entry2 = new (buffer) Entry(2, inst, 64);
    page->add_entry(*entry2);

    auto entry0 = new (buffer) Entry(0, inst, 64);
    page->add_entry(*entry0);

    page->sort();

    auto res0 = page->read_entry(0);
    BOOST_CHECK_EQUAL(res0->param_id, 0);

    auto res1 = page->read_entry(1);
    BOOST_CHECK_EQUAL(res1->param_id, 1);

    auto res2 = page->read_entry(2);
    BOOST_CHECK_EQUAL(res2->param_id, 2);
}


static PageHeader* init_search_range_test(char* page_ptr, int page_len, int num_values) {
    auto page = new (page_ptr) PageHeader(PageType::Index, 0, page_len, 0);
    char buffer[64];

    for(int i = 0; i < num_values; i++) {
        TimeStamp inst = {1000L + i};
        auto entry = new (buffer) Entry(1, inst, 64);
        entry->value[0] = i;
        BOOST_CHECK(page->add_entry(*entry) != AKU_WRITE_STATUS_OVERFLOW);
    }

    page->sort();

    return page;
}


BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_0)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {1000L}, {1067L}, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 68);

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], 67 - i);
        BOOST_CHECK(entry->time.precise >= 1000L);
        BOOST_CHECK(entry->time.precise <= 1067L);
    }
}


BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_1)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {1010L}, {1050L}, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 41);
    std::vector<int64_t> timestamps;

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], 50 - i);
        BOOST_CHECK(entry->time.precise >= 1010L);
        BOOST_CHECK(entry->time.precise <= 1050L);
        timestamps.push_back(entry->time.precise);
    }

    // Check forward time direction
    BOOST_CHECK(timestamps.front() > timestamps.back());
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_2)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 100);

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], 99 - i);
        BOOST_CHECK(entry->time.precise >= 1000L);
        BOOST_CHECK(entry->time.precise <= 1100L);
    }
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_3)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {2000L}, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 1);
    const Entry* entry = page->read_entry(indexes[0]);
    BOOST_CHECK_EQUAL(entry->value[0], 99);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_4)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(2, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 0);
}

// Forward direction search
BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_0)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {1000L}, {1067L}, AKU_CURSOR_DIR_FORWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 68);

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], i);
        BOOST_CHECK(entry->time.precise >= 1000L);
        BOOST_CHECK(entry->time.precise <= 1067L);
    }
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_1)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {1010L}, {1050L}, AKU_CURSOR_DIR_FORWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 41);
    std::vector<int64_t> timestamps;

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], 10 + i);
        BOOST_CHECK(entry->time.precise >= 1010L);
        BOOST_CHECK(entry->time.precise <= 1050L);
        timestamps.push_back(entry->time.precise);
    }

    // Check forward time direction
    BOOST_CHECK(timestamps.front() < timestamps.back());
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_2)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 100);

    for(int i = 0; i < cursor.results_num; i++) {
        const Entry* entry = page->read_entry(indexes[i]);
        BOOST_CHECK_EQUAL(entry->value[0], i);
        BOOST_CHECK(entry->time.precise >= 1000L);
        BOOST_CHECK(entry->time.precise <= 1100L);
    }
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_3)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {2000L}, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 1);
    const Entry* entry = page->read_entry(indexes[0]);
    BOOST_CHECK_EQUAL(entry->value[0], 99);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_4)
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(2, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    page->search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 0);
}

// TODO: add tests for more irregullar data distribution and randomized tests
