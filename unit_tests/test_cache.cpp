#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>  // for barrier
#include <thread>

#include "cache.h"

#include "cpp-btree/btree_map.h"


using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_seq_search_backward) {
    Sequence seq;

    for (int i = 0; i < 1000; ++i) {
        TimeStamp ts = {1000L + i};
        seq.add(ts, 1, (EntryOffset)i);
    }

    SearchQuery query(1, {1400L}, {1499L}, AKU_CURSOR_DIR_BACKWARD);
    Caller caller;
    RecordingCursor cursor;

    std::vector<EntryOffset> results;
    seq.search(caller, &cursor, query);
    for (size_t i = 0; i < cursor.offsets.size(); i++) {
        results.push_back(cursor.offsets[i]);
    }

    BOOST_CHECK_EQUAL(results.size(), 100);

    for(int i = 0; i < 100; i++) {
        BOOST_REQUIRE_EQUAL(results[i], 499 - i);
    }
}

BOOST_AUTO_TEST_CASE(Test_seq_search_forward) {
    Sequence seq;

    for (int i = 0; i < 1000; ++i) {
        TimeStamp ts = {1000L + i};
        seq.add(ts, 1, (EntryOffset)i);
    }

    SearchQuery query(1, {1400L}, {1499L}, AKU_CURSOR_DIR_FORWARD);
    Caller caller;
    RecordingCursor cursor;

    std::vector<EntryOffset> results;
    seq.search(caller, &cursor, query);
    for (size_t i = 0; i < cursor.offsets.size(); i++) {
        results.push_back(cursor.offsets[i]);
    }

    BOOST_CHECK_EQUAL(results.size(), 100);

    for(int i = 0; i < 100; i++) {
        BOOST_REQUIRE_EQUAL(results[i], 400 + i);
    }
}

// --------- Cache tests -----------

BOOST_AUTO_TEST_CASE(Test_cache_search_bad_direction) {
    const int N = 10000;
    Cache cache({1000L}, N);
    SearchQuery query(1, {1400L}, {1500L}, 111);
    Caller caller;
    RecordingCursor cursor;
    cache.search(caller, &cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.completed, false);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_cache_search_bad_time) {
    const int N = 10000;
    Cache cache({1000L}, N);
    SearchQuery query(1, {1200L}, {1000L}, AKU_CURSOR_DIR_BACKWARD);
    Caller caller;
    RecordingCursor cursor;
    cache.search(caller, &cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.completed, false);
    BOOST_REQUIRE_EQUAL(cursor.error_code, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_cache_max_size) {
    const int N = 10000;
    Cache cache({1000L}, N);
    char entry_buffer[0x100];
    TimeStamp ts = {100001L};
    Entry* entry = new (entry_buffer) Entry(1u, ts, Entry::get_size(4));
    size_t swapped = 0u;
    int status = AKU_SUCCESS, prev_status = AKU_SUCCESS;
    for(int i = 0; i < N*2; i++) {
        status = cache.add_entry(*entry, 0, &swapped);
        if (status == AKU_SUCCESS && prev_status != AKU_SUCCESS) {
            BOOST_FAIL("LimitCounter error");
        }
        if (status != AKU_SUCCESS && prev_status == AKU_SUCCESS) {
            BOOST_REQUIRE_NE(i, 0);
        }
        prev_status = status;
    }
    BOOST_REQUIRE_EQUAL(status, AKU_EOVERFLOW);
}

BOOST_AUTO_TEST_CASE(Test_cache_late_write) {
    const int64_t N = 4096L;
    Cache cache({N}, 10000000);
    char entry_buffer[0x100];
    int64_t time = 0x10000L;
    TimeStamp ts = {time};
    Entry* entry = new (entry_buffer) Entry(1u, ts, Entry::get_size(4));
    int status = AKU_SUCCESS;
    size_t swaps = 0;
    status = cache.add_entry(*entry, 0, &swaps);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {time + 2L};  // future write is possible
    status = cache.add_entry(*entry, 1, &swaps);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {time - N};
    status = cache.add_entry(*entry, 2, &swaps);
    BOOST_CHECK(status == AKU_SUCCESS);
    entry->time = {time - N - (N / AKU_LIMITS_MAX_CACHES)};
    status = cache.add_entry(*entry, 3, &swaps);
    BOOST_CHECK(status == AKU_ELATE_WRITE);
}

static int init_search_range_test(Cache* cache, int64_t start, int num_values) {
    char buffer[64];
    int num_overflows = 0;
    for(int i = 0; i < num_values; i++) {
        TimeStamp inst = {start + i};
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

void generic_cache_test
                 ( int64_t begin
                 , int64_t end
                 , int direction
                 , size_t expected_size
                 , EntryOffset expected_first
                 , EntryOffset expected_last
                 , EntryOffset expected_offset_skew
                 )
{
    Cache cache({100L}, 100000);
    init_search_range_test(&cache, 0L, 10000);

    SearchQuery query(1, {begin}, {end}, direction);
    RecordingCursor cursor;
    Caller caller;

    cache.search(caller, &cursor, query);

    BOOST_CHECK_EQUAL(cursor.completed, true);
    BOOST_CHECK_EQUAL(cursor.offsets.size(), expected_size);
    if (expected_size == 0) {
        return;
    }
    BOOST_CHECK_EQUAL(cursor.offsets[0], expected_first);
    BOOST_CHECK_EQUAL(cursor.offsets[cursor.offsets.size()-1], expected_last);

    for(size_t i = 0; i < cursor.offsets.size(); i++) {
        if (direction == AKU_CURSOR_DIR_FORWARD) {
            BOOST_CHECK_EQUAL(cursor.offsets[i], expected_offset_skew + i);
        } else {
            BOOST_CHECK_EQUAL(cursor.offsets[i], expected_offset_skew - i);
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_0)
{
    generic_cache_test(1000L, 4999L, AKU_CURSOR_DIR_BACKWARD, 4000, 4999, 1000, 4999);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_1)
{
    generic_cache_test(8000L, TimeStamp::MAX_TIMESTAMP.value, AKU_CURSOR_DIR_BACKWARD, 2000, 9999, 8000, 9999);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_2)
{
    generic_cache_test(TimeStamp::MIN_TIMESTAMP.value, 999L, AKU_CURSOR_DIR_BACKWARD, 1000, 999, 0, 999);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_3)
{
    generic_cache_test(20000L, TimeStamp::MAX_TIMESTAMP.value, AKU_CURSOR_DIR_BACKWARD, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_backward_4)
{
    generic_cache_test(-1000L, -1L, AKU_CURSOR_DIR_BACKWARD, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_forward_0)
{
    generic_cache_test(2000L, 7999L, AKU_CURSOR_DIR_FORWARD, 6000, 2000, 7999, 2000);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_forward_1)
{
    generic_cache_test(TimeStamp::MIN_TIMESTAMP.value, 999L, AKU_CURSOR_DIR_FORWARD, 1000, 0, 999, 0);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_forward_2)
{
    generic_cache_test(9000L, TimeStamp::MAX_TIMESTAMP.value, AKU_CURSOR_DIR_FORWARD, 1000, 9000, 9999, 9000);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_forward_3)
{
    generic_cache_test(20000L, TimeStamp::MAX_TIMESTAMP.value, AKU_CURSOR_DIR_FORWARD, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(Test_CacheSingleParamCursor_search_range_forward_4)
{
    generic_cache_test(-10000L, -1L, AKU_CURSOR_DIR_FORWARD, 0, 0, 0, 0);
}

// ------------------ Test Bucket --------------------- //

void test_bucket_merge(int n, int len) {

    auto page_len = 0x100*len*n;
    char buffer[page_len];
    PageHeader* page = new (buffer) PageHeader(PageType::Index, 0, page_len, 0);
    Bucket bucket(1000000, 0L);

    boost::barrier enter(n), insert(n + 1), exit(n + 1);
    std::mutex m;

    // generate data
    auto fn = [&m, len, page, &bucket, &enter, &insert, &exit] () {
        enter.wait();
        for (uint32_t i = 0; i < len; i++) {
            auto rval = rand();
            auto param_id = rval & 3;
            auto ts = rval >> 2;
            char entry_buf[0x100];
            Entry* entry = new(entry_buf) Entry(param_id, {ts}, Entry::get_size(4));
            entry->value[0] = i;
            std::unique_lock<std::mutex> l(m);
            auto status = page->add_entry(*entry);
            auto offset = page->last_offset;
            l.unlock();
            if(status != AKU_SUCCESS) {
                throw std::runtime_error(aku_error_message(status));
            }
            status = bucket.add({ts}, param_id, offset);
            if(status != AKU_SUCCESS) {
                throw std::runtime_error(aku_error_message(status));
            }
        }
        insert.wait();
        exit.wait();
    };

    boost::thread_group tgroup;
    for (int t = 0; t < n; t++) {
        tgroup.create_thread(fn);
    }

    // run merge
    insert.wait();

    RecordingCursor cursor;
    bucket.state++;
    Caller c;
    int status = bucket.merge(c, &cursor);
    BOOST_CHECK_EQUAL(status, AKU_SUCCESS);

    exit.wait();
    tgroup.join_all();

    // all offsets must be in increasing order
    BOOST_CHECK_EQUAL(cursor.offsets.size(), len*n);
    int64_t prev = 0L;
    int counter = 0;
    for(auto offset: cursor.offsets) {
        auto entry = page->read_entry(offset);
        auto curr = entry->time.value;
        if (prev <= curr) {
            prev = curr;
        } else {
            BOOST_FAIL("Invalid timestamp");
        }
        counter++;
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

void test_bucket_search(int n, int len) {

    Bucket bucket(100000, 0L);

    boost::barrier enter(n), insert(n + 1), exit(n + 1);
    std::mutex m;
    std::multimap<std::tuple<int64_t, ParamId>, EntryOffset> expected;

    // generate data
    auto fn = [&] () {
        enter.wait();
        for (unsigned i = 0; i < len; i++) {
            auto rval = rand();
            ParamId param_id = rval & 3;
            int64_t ts = rval >> 2;
            auto status = bucket.add({ts}, param_id, (EntryOffset)i);
            if (status != AKU_SUCCESS) {
                throw std::runtime_error(aku_error_message(status));
            }

            std::lock_guard<std::mutex> l(m);
            expected.insert(
                        std::make_pair(
                            std::make_tuple(ts, param_id),
                            (EntryOffset)i
                            ));
        }
        insert.wait();
        exit.wait();
    };

    std::vector<std::thread> tgroup;
    for (int t = 0; t < n; t++) {
        tgroup.emplace_back(fn);
    }

    insert.wait();

    RecordingCursor cursor;
    Caller caller;
    auto pred = [](ParamId) {return SearchQuery::MATCH;};
    SearchQuery query(pred, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD);
    bucket.search(caller, &cursor, query);

    exit.wait();

    for(auto& t: tgroup) {
        t.join();
    }

    BOOST_REQUIRE_EQUAL(cursor.offsets.size(), n*len);
    BOOST_REQUIRE_EQUAL(cursor.error_code, RecordingCursor::NO_ERROR);

    int cnt = 0;
    for(auto it: expected) {
        EntryOffset value = it.second;
        BOOST_REQUIRE_EQUAL(cursor.offsets[cnt], value);
        cnt++;
    }
}

BOOST_AUTO_TEST_CASE(Test_bucket_search_1)
{
    test_bucket_search(1, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_search_2)
{
    test_bucket_search(2, 1000);
}

BOOST_AUTO_TEST_CASE(Test_bucket_search_4)
{
    test_bucket_search(4, 1000);
}
