#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"

#include "cpp-btree/btree_map.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_seqeration_insert_overflow_by_size)
{
    Sequence seq(4);

    TimeStamp ts = { (int64_t)0 };
    BOOST_REQUIRE(seq.add(ts, (ParamId)1, (EntryOffset)1) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(seq.add(ts, (ParamId)2, (EntryOffset)2) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(seq.add(ts, (ParamId)3, (EntryOffset)3) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(seq.add(ts, (ParamId)4, (EntryOffset)4) == AKU_WRITE_STATUS_SUCCESS);
    BOOST_REQUIRE(seq.add(ts, (ParamId)5, (EntryOffset)5) == AKU_WRITE_STATUS_OVERFLOW);
    BOOST_REQUIRE(seq.add(ts, (ParamId)6, (EntryOffset)6) == AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(Test_seq_search_backward) {
    Sequence seq(10000);

    for (int i = 0; i < 1000; ++i) {
        TimeStamp ts = {1000L + i};
        seq.add(ts, 1, (EntryOffset)i);
    }

    EntryOffset indexes[10];
    SingleParameterCursor cursor(1, {1400L}, {1500L}, AKU_CURSOR_DIR_BACKWARD, indexes, 10);

    std::vector<EntryOffset> results;
    while(cursor.state != AKU_CURSOR_COMPLETE) {
        seq.search(&cursor);
        for (int i = 0; i < cursor.results_num; i++) {
            results.push_back(indexes[i]);
        }
    }

    BOOST_CHECK_EQUAL(results.size(), 100);

    for(int i = 0; i < 100; i++) {
        BOOST_REQUIRE_EQUAL(results[i], 500 - i);
    }
}

BOOST_AUTO_TEST_CASE(Test_seq_search_forward) {
    Sequence seq(10000);

    for (int i = 0; i < 1000; ++i) {
        TimeStamp ts = {1000L + i};
        seq.add(ts, 1, (EntryOffset)i);
    }

    EntryOffset indexes[10];
    SingleParameterCursor cursor(1, {1400L}, {1500L}, AKU_CURSOR_DIR_FORWARD, indexes, 10);

    std::vector<EntryOffset> results;
    while(cursor.state != AKU_CURSOR_COMPLETE) {
        seq.search(&cursor);
        for (int i = 0; i < cursor.results_num; i++) {
            results.push_back(indexes[i]);
        }
    }

    BOOST_CHECK_EQUAL(results.size(), 100);

    for(int i = 0; i < 100; i++) {
        BOOST_REQUIRE_EQUAL(results[i], 400 + i);
    }
}

BOOST_AUTO_TEST_CASE(Test_seq_search_bad_direction) {
    Sequence seq(10000);
    EntryOffset indexes[10];
    SingleParameterCursor cursor(1, {1400L}, {1500L}, 111, indexes, 10);
    seq.search(&cursor);
    BOOST_REQUIRE_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_seq_search_bad_time) {
    Sequence seq(10000);
    EntryOffset indexes[10];
    SingleParameterCursor cursor(1, {1200L}, {1000L}, AKU_CURSOR_DIR_BACKWARD, indexes, 10);
    seq.search(&cursor);
    BOOST_REQUIRE_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_seq_search_bad_buffer) {
    Sequence seq(10000);
    SingleParameterCursor cursor(1, {1000L}, {1500L}, AKU_CURSOR_DIR_BACKWARD, nullptr, 10);
    seq.search(&cursor);
    BOOST_REQUIRE_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_seq_search_bad_buffer_size) {
    Sequence seq(10000);
    EntryOffset indexes[10];
    SingleParameterCursor cursor(1, {1200L}, {1000L}, AKU_CURSOR_DIR_BACKWARD, indexes, 0);
    seq.search(&cursor);
    BOOST_REQUIRE_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

// --------- Cache tests -----------

BOOST_AUTO_TEST_CASE(Test_cache_dump_by_max_size) {
    return;
    Cache cache({1000L}, 3);
    char entry_buffer[0x100];
    TimeStamp ts = {100001L};
    Entry* entry = new (entry_buffer) Entry(1u, ts, Entry::get_size(4));
    int status = AKU_SUCCESS;
    size_t swapped = 0u;
    status = cache.add_entry(*entry, 0, &swapped);
    BOOST_CHECK(status == AKU_SUCCESS);
    BOOST_CHECK(swapped == AKU_LIMITS_MAX_CACHES);
    swapped = 0;
    entry->time = {100002L};
    status = cache.add_entry(*entry, 1, &swapped);
    BOOST_CHECK(swapped == 0u);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {100003L};
    status = cache.add_entry(*entry, 2, &swapped);
    BOOST_CHECK(swapped == 0u);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {100004L};
    status = cache.add_entry(*entry, 3, &swapped);
    BOOST_CHECK(swapped == 0u);
    BOOST_CHECK(status == AKU_EOVERFLOW);
    entry->time = {100500L};
    status = cache.add_entry(*entry, 4, &swapped);  // future write
    BOOST_CHECK(swapped == 1u);
    BOOST_CHECK(status == AKU_SUCCESS);
}

BOOST_AUTO_TEST_CASE(Test_cache_late_write) {
    return;
    Cache cache({1000L}, 1000);
    char entry_buffer[0x100];
    int64_t time = apr_time_now();
    TimeStamp ts = {time};
    Entry* entry = new (entry_buffer) Entry(1u, ts, Entry::get_size(4));
    int status = AKU_SUCCESS;
    size_t swaps = 0;
    status = cache.add_entry(*entry, 0, &swaps);
    BOOST_CHECK(status == AKU_SUCCESS);
    BOOST_CHECK(swaps == AKU_LIMITS_MAX_CACHES);
    swaps = 0;
    entry->time = {time + 2L};
    status = cache.add_entry(*entry, 1, &swaps);
    BOOST_CHECK(swaps == 0u);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {time - 10000L};
    status = cache.add_entry(*entry, 2, &swaps);  // late write
    BOOST_CHECK(swaps == 0u);
    BOOST_CHECK(status == AKU_EOVERFLOW);
}

static int init_search_range_test(Cache* cache, int num_values) {
    char buffer[64];
    int num_overflows = 0;
    for(int i = 0; i < num_values; i++) {
        TimeStamp inst = {1000L + i};
        auto entry = new (buffer) Entry(1, inst, 64);
        entry->value[0] = i;
        size_t nswaps = 0;
        int stat = cache->add_entry(*entry, i, &nswaps);
        BOOST_CHECK(stat == AKU_WRITE_STATUS_OVERFLOW || stat == AKU_WRITE_STATUS_SUCCESS);
        if (stat == AKU_WRITE_STATUS_OVERFLOW) {
            num_overflows++;
        }
    }
    return num_overflows;
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_0)
{
    return;
    char page_ptr[0x10000];
    Cache cache({1000000L}, 100000);
    init_search_range_test(&cache, 100);

    uint32_t indexes[1000];
    SingleParameterCursor cursor(1, {1000L}, {1067L}, AKU_CURSOR_DIR_BACKWARD, indexes, 1000);

    cache.search(&cursor);

    BOOST_CHECK_EQUAL(cursor.state, AKU_CURSOR_COMPLETE);
    BOOST_CHECK_EQUAL(cursor.results_num, 68);

    for(int i = 0; i < cursor.results_num; i++) {
        BOOST_CHECK_EQUAL(indexes[i], i);
    }
}

// ------------------ Test Bucket --------------------- //

void test_bucket_merge(int n, int len) {

    auto page_len = 0x100*len*n;
    char buffer[page_len];
    PageHeader* page = new (buffer) PageHeader(PageType::Index, 0, page_len, 0);
    Bucket bucket(n, len*2, 0L);


    // generate data

    for (unsigned i = 0; i < len; i++) {
        auto rval = rand();
        auto param_id = rval & 3;
        auto ts = rval >> 2;
        auto val = i;
        char entry_buf[0x100];
        Entry* entry = new(entry_buf) Entry(param_id, {ts}, Entry::get_size(4));
        entry->value[0] = val;
        page->add_entry(*entry);
        bucket.add({ts}, param_id, page->last_offset);
    }

    // run merge

    RecordingCursor cursor;
    bucket.state++;
    int status = bucket.merge(&cursor, page);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    // all offsets must be in increasing order
    BOOST_REQUIRE(cursor.offsets.size() != 0);
    int64_t prev = 0L;
    for(auto offset: cursor.offsets) {
        auto entry = page->read_entry(offset);
        auto curr = entry->time.value;
        BOOST_REQUIRE_LT(prev, curr);
        prev = curr;
    }
}


BOOST_AUTO_TEST_CASE(Test_bucket_merge_1)
{
    test_bucket_merge(1, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_merge_2)
{
    test_bucket_merge(2, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_merge_3)
{
    test_bucket_merge(3, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_merge_4)
{
    test_bucket_merge(4, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_merge_8)
{
    test_bucket_merge(8, 1000);
}
