#include <iostream>

#define BOOST_TEST_MODULE IndexTests
#include <boost/test/included/unit_test.hpp>

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

    auto centry = page->find_entry(0);
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

    auto res0 = page->find_entry(0);
    BOOST_CHECK_EQUAL(res0->param_id, 0);

    auto res1 = page->find_entry(1);
    BOOST_CHECK_EQUAL(res1->param_id, 1);

    auto res2 = page->find_entry(2);
    BOOST_CHECK_EQUAL(res2->param_id, 2);
}



