#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>
#include <iostream>

#include "akumuli_def.h"
#include "cursor.h"
#include "page.h"

using namespace Akumuli;

struct AkumuliInitializer {
    AkumuliInitializer() {
        aku_initialize();
    }
};

AkumuliInitializer initializer;

BOOST_AUTO_TEST_CASE(TestPaging1)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    BOOST_CHECK_EQUAL(0, page->get_entries_count());
}

BOOST_AUTO_TEST_CASE(TestPaging2)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    auto free_space_before = page->get_free_space();
    char buffer[128];
    aku_MemRange range = {buffer, 128};
    auto result = page->add_entry(1, 2, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto free_space_after = page->get_free_space();
    BOOST_CHECK_EQUAL(free_space_before - free_space_after, sizeof(aku_Entry) + 128 + sizeof(aku_EntryOffset));
}

BOOST_AUTO_TEST_CASE(TestPaging3)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    aku_MemRange range = {nullptr, 4096};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(TestPaging4)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    aku_MemRange range = {nullptr, 0};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_BAD_DATA);
}

BOOST_AUTO_TEST_CASE(TestPaging5)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    char buffer[222];
    aku_MemRange range = {buffer, 222};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto len = page->get_entry_length_at(0);
    BOOST_CHECK_EQUAL(len, 222);
}

BOOST_AUTO_TEST_CASE(TestPaging6)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    uint32_t buffer[] = {0, 1, 2, 3, 4, 5, 6, 7};
    aku_TimeStamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(uint32_t)*sizeof(buffer)};
    auto result = page->add_entry(3333, inst, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);

    char out_buffer[0x1000];
    aku_Entry* entry = reinterpret_cast<aku_Entry*>(out_buffer);
    entry->length = 0x1000 - sizeof(aku_Entry);
    int len = page->copy_entry_at(0, entry);
    BOOST_CHECK_EQUAL(len, range.length);
    BOOST_CHECK_EQUAL(entry->length, range.length);
    BOOST_CHECK_EQUAL(entry->param_id, 3333);
}

BOOST_AUTO_TEST_CASE(TestPaging7)
{
    char page_ptr[4096]; 
    auto page = new (page_ptr) PageHeader(0, 4096, 0);
    uint32_t buffer[] = {1, 2, 3, 4};
    aku_TimeStamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(buffer)*sizeof(uint32_t)};
    auto result = page->add_entry(3333, inst, range);

    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);

    auto centry = page->read_entry_at(0);
    BOOST_CHECK_EQUAL(centry->length, range.length);
    BOOST_CHECK_EQUAL(centry->param_id, 3333);
}


static PageHeader* init_search_range_test(char* page_ptr, int page_len, int num_values) {
    auto page = new (page_ptr) PageHeader(0, page_len, 0);

    for(uint32_t i = 0u; i < (uint32_t)num_values; i++) {
        aku_TimeStamp inst = 1000L + i;
        uint32_t box[1] = {i};
        aku_MemRange range = {(void*)box, sizeof(uint32_t)};
        aku_ParamId id = 1;
        BOOST_CHECK(page->add_entry(id, inst, range) != AKU_WRITE_STATUS_OVERFLOW);
    }

    page->_sort();

    return page;
}

struct ExpectedSearchResults {
    bool completed;
    int error_code;
    size_t ressize;
    aku_EntryOffset skew;
};

void generic_search_test
    ( int param_id
    , aku_TimeStamp begin
    , aku_TimeStamp end
    , int direction
    , ExpectedSearchResults const& expectations
    )
{
    char page_ptr[0x10000];
    auto page = init_search_range_test(page_ptr, 0x10000, 100);
    SearchQuery query(param_id, begin, end, direction);
    RecordingCursor cursor;
    Caller caller;

    page->search(caller, &cursor, query);

    BOOST_CHECK_EQUAL(cursor.completed, expectations.completed);
    BOOST_CHECK_EQUAL(cursor.error_code, expectations.error_code);

    if (expectations.error_code != RecordingCursor::NO_ERROR) {
        return;
    }

    BOOST_CHECK_EQUAL(cursor.offsets.size(), expectations.ressize);

    for(size_t i = 0; i < cursor.offsets.size(); i++) {
        const aku_Entry* entry = page->read_entry(cursor.offsets[i].first);
        if (direction == AKU_CURSOR_DIR_BACKWARD) {
            BOOST_CHECK_EQUAL(entry->value[0], expectations.skew - i);
        } else {
            BOOST_CHECK_EQUAL(entry->value[0], expectations.skew + i);
        }
        BOOST_CHECK_GE(entry->time, begin);
        BOOST_CHECK_LE(entry->time, end);
    }
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_0)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 60;
    expectations.skew = 59;
    generic_search_test(1, {1000L}, {1059L}, AKU_CURSOR_DIR_BACKWARD, expectations);
}


BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_1)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 50;
    expectations.skew = 59;
    generic_search_test(1, {1010L}, {1059L}, AKU_CURSOR_DIR_BACKWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_2)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 100;
    expectations.skew = 99;
    generic_search_test(1, AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_3)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 0;
    expectations.skew = 0;
    generic_search_test(1, {2000L}, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_4)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 0;
    expectations.skew = 0;
    generic_search_test(2, AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_BACKWARD, expectations);
}

// Forward direction search
BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_0)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 70;
    expectations.skew = 0;
    generic_search_test(1, {1000L}, {1069L}, AKU_CURSOR_DIR_FORWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_1)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 60;
    expectations.skew = 10;
    generic_search_test(1, {1010L}, {1069L}, AKU_CURSOR_DIR_FORWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_2)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 100;
    expectations.skew = 0;
    generic_search_test(1, AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_3)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 0;
    expectations.skew = 0;
    generic_search_test(1, {2000L}, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD, expectations);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_4)
{
    ExpectedSearchResults expectations;
    expectations.completed = true;
    expectations.error_code = RecordingCursor::NO_ERROR;
    expectations.ressize = 0;
    expectations.skew = 0;
    generic_search_test(2, AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD, expectations);
}

static PageHeader* init_search_range_test_with_skew(char* page_ptr, int page_len, int num_values, int time_skew) {
    auto page = new (page_ptr) PageHeader(0, page_len, 0);
    for(int i = 0; i < num_values; i++) {
        aku_TimeStamp inst = 1000L + i*time_skew;
        aku_MemRange range = {(void*)&i, sizeof(i)};
        BOOST_CHECK(page->add_entry(1, inst, range) != AKU_WRITE_STATUS_OVERFLOW);
    }
    page->_sort();
    return page;
}

void generic_search_test_with_skew
     ( int param_id
     , aku_TimeStamp begin
     , aku_TimeStamp end
     , int direction
     , ExpectedSearchResults const& expectations
     )
{
    char page_ptr[0x10000];
    auto page = init_search_range_test_with_skew(page_ptr, 0x10000, 1000, 2);

    SearchQuery query(param_id, begin, end, direction);
    RecordingCursor cursor;
    Caller caller;

    page->search(caller, &cursor, query);

    BOOST_CHECK_EQUAL(cursor.completed, expectations.completed);
    BOOST_CHECK_EQUAL(cursor.error_code, expectations.error_code);

    if (expectations.error_code != RecordingCursor::NO_ERROR)
        return;

    BOOST_CHECK_EQUAL(cursor.offsets.size(), expectations.ressize);

    std::vector<int64_t> timestamps;
    for(size_t i = 0; i < cursor.offsets.size(); i++) {
        const aku_Entry* entry = page->read_entry(cursor.offsets[i].first);
        BOOST_CHECK_GE(entry->time, begin);
        BOOST_CHECK_LE(entry->time, end);
        timestamps.push_back(entry->time);
    }

    if (direction == AKU_CURSOR_DIR_FORWARD) {
        BOOST_CHECK(timestamps.front() < timestamps.back());
    } else {
        BOOST_CHECK(timestamps.front() > timestamps.back());
    }
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_forward_with_skew_0)
{
    ExpectedSearchResults e;
    e.completed = true;
    e.error_code = RecordingCursor::NO_ERROR;
    e.ressize = 500;
    generic_search_test_with_skew(1, {1010L}, {2008L}, AKU_CURSOR_DIR_FORWARD, e);
}

BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_backward_with_skew_0)
{
    ExpectedSearchResults e;
    e.completed = true;
    e.error_code = RecordingCursor::NO_ERROR;
    e.ressize = 500;
    generic_search_test_with_skew(1, {1010L}, {2008L}, AKU_CURSOR_DIR_BACKWARD, e);
}

// TODO: test multi-part search calls
BOOST_AUTO_TEST_CASE(Test_SingleParamCursor_search_range_large)
{
    const int               buf_len = 1024*1024*8;
    std::vector<char>       buffer(buf_len);
    std::vector<uint64_t>    timestamps;
    std::vector<aku_ParamId>    paramids;
    int64_t                 time_stamp = 0L;
    PageHeader*             page = nullptr;

    page = new (&buffer[0]) PageHeader(0, buf_len, 0);

    for(int i = 0; true; i++)
    {
        int rand_num = rand();
        aku_TimeStamp inst = time_stamp;
        aku_ParamId id = 1 + (rand_num & 1);
        aku_MemRange range = {(void*)&i, sizeof(i)};
        if(page->add_entry(id, inst, range) == AKU_WRITE_STATUS_OVERFLOW) {
            break;
        }
        timestamps.push_back(time_stamp);  // i-th timestamp
        paramids.push_back(id);
        // timestamp grows always
        time_stamp += 1 + rand_num % 100;
    }

    page->_sort();

    for (int round = 0; round < 10; round++) {
        // select start timestamp
        int directions[] = {
            AKU_CURSOR_DIR_FORWARD,
            AKU_CURSOR_DIR_BACKWARD
        };
        int dir = directions[rand() & 1];
        double rnd_double = 0.001*((rand() + 1) % 200);
        aku_TimeStamp start_time = (uint64_t)(rnd_double*page->bbox.max_timestamp);
        aku_TimeStamp stop_time  = (uint64_t)((rnd_double + 0.6)*page->bbox.max_timestamp);
        aku_ParamId id2search = 1 + (rand() & 1);
        BOOST_REQUIRE(start_time > 0 && start_time < page->bbox.max_timestamp);
        BOOST_REQUIRE(stop_time > 0 && stop_time < page->bbox.max_timestamp);
        BOOST_REQUIRE(stop_time > start_time);
        SearchQuery query(id2search, start_time, stop_time, dir);
        Caller caller;
        RecordingCursor cursor;
        std::vector<uint32_t> matches;
        page->search(caller, &cursor, query);
        for(size_t i = 0; i < cursor.offsets.size(); i++) {
            auto offset = cursor.offsets.at(i).first;
            const aku_Entry* entry = page->read_entry(offset);
            auto index = entry->value[0];
            matches.push_back(index);
            BOOST_REQUIRE_EQUAL(entry->time, timestamps[index]);
            BOOST_REQUIRE(entry->param_id == paramids[index]);
            BOOST_REQUIRE(entry->value[0] == (uint32_t)index);
        }
        // Check
        size_t match_index = 0u;
        if (dir == AKU_CURSOR_DIR_FORWARD) {
            for(size_t i = 0u; i < timestamps.size(); i++) {
                auto curr_id = paramids[i];
                auto curr_ts = timestamps[i];
                if (curr_id == id2search) {
                    if (curr_ts >= start_time && curr_ts <= stop_time) {
                        auto expected_index = matches[match_index];
                        BOOST_REQUIRE_EQUAL(i, expected_index);
                        match_index++;
                    }
                }
            }
        } else {
            for(int i = (int)timestamps.size() - 1; i >= 0; i--) {
                auto curr_id = paramids[i];
                auto curr_ts = timestamps[i];
                if (curr_id == id2search) {
                    if (curr_ts >= start_time && curr_ts <= stop_time) {
                        auto expected_index = matches[match_index];
                        BOOST_REQUIRE_EQUAL(i, expected_index);
                        match_index++;
                    }
                }
            }
        }
        BOOST_REQUIRE_EQUAL(match_index, matches.size());
    }
}
