#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <apr.h>
#include <vector>
#include <iostream>

#include "akumuli_def.h"
#include "cursor.h"
#include "page.h"

using namespace Akumuli;

struct AkumuliInitializer {
    AkumuliInitializer() {
        apr_initialize();
    }
};

namespace {

/** Simple cursor implementation for testing.
  * Stores all values in std::vector.
  */
struct RecordingCursor : InternalCursor {
    std::vector<CursorResult> results;
    bool completed = false;
    enum ErrorCodes {
        NO_ERROR = -1
    };
    int error_code = NO_ERROR;

    virtual bool put(Caller&, CursorResult const& result) {
        results.push_back(result);
        return true;
    }

    virtual void complete(Caller&) {
        completed = true;
    }

    virtual void set_error(Caller&, int error_code) {
        this->error_code = error_code;
    }
};

}  // namespace

AkumuliInitializer initializer;

BOOST_AUTO_TEST_CASE(TestPaging1) {

    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    BOOST_CHECK_EQUAL(0, page->get_entries_count());
    BOOST_CHECK_EQUAL(4096, page->get_free_space());
}

BOOST_AUTO_TEST_CASE(TestPaging2) {

    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    auto free_space_before = page->get_free_space();
    char buffer[128];
    aku_MemRange range = {buffer, 128};
    auto result = page->add_entry(1, 2, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto free_space_after = page->get_free_space();
    BOOST_CHECK_EQUAL(free_space_before - free_space_after, sizeof(aku_Entry) + 128 + sizeof(aku_EntryIndexRecord));
}

BOOST_AUTO_TEST_CASE(TestPaging3)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    aku_MemRange range = {nullptr, static_cast<uint32_t>(page_mem.size())};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_OVERFLOW);
}

BOOST_AUTO_TEST_CASE(TestPaging4)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    aku_MemRange range = {nullptr, 0};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_BAD_DATA);
}

BOOST_AUTO_TEST_CASE(TestPaging5)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    char buffer[222];
    aku_MemRange range = {buffer, 222};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);
    auto len = page->get_entry_length_at(0);
    BOOST_CHECK_EQUAL(len, 222);
}

BOOST_AUTO_TEST_CASE(TestPaging6)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    uint32_t buffer[] = {0, 1, 2, 3, 4, 5, 6, 7};
    aku_Timestamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(buffer)};
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
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);
    uint32_t buffer[] = {1, 2, 3, 4};
    aku_Timestamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(buffer)};
    auto result = page->add_entry(3333, inst, range);

    BOOST_CHECK_EQUAL(result, AKU_WRITE_STATUS_SUCCESS);

    auto centry = page->read_entry_at(0);
    BOOST_CHECK_EQUAL(centry->length, range.length);
    BOOST_CHECK_EQUAL(centry->param_id, 3333);
}


static PageHeader* init_search_range_test(char* page_ptr, int page_len, int num_values) {
    auto page = new (page_ptr) PageHeader(0, page_len, 0);

    for(uint32_t i = 0u; i < (uint32_t)num_values; i++) {
        aku_Timestamp inst = 1000L + i;
        uint32_t box[1] = {i};
        aku_MemRange range = {(void*)box, sizeof(uint32_t)};
        aku_ParamId id = 1;
        BOOST_CHECK(page->add_entry(id, inst, range) != AKU_WRITE_STATUS_OVERFLOW);
    }

    return page;
}

struct ExpectedSearchResults {
    bool completed;
    int error_code;
    size_t ressize;
    uint32_t skew;
};

void generic_search_test
    ( int param_id
    , aku_Timestamp begin
    , aku_Timestamp end
    , int direction
    , ExpectedSearchResults const& expectations
    )
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 0x10000);
    auto page = init_search_range_test(page_mem.data(), page_mem.size(), 100);
    SearchQuery query(param_id, begin, end, direction);
    RecordingCursor cursor;
    Caller caller;

    page->search(caller, &cursor, query);

    BOOST_CHECK_EQUAL(cursor.completed, expectations.completed);
    BOOST_CHECK_EQUAL(cursor.error_code, expectations.error_code);

    if (expectations.error_code != RecordingCursor::NO_ERROR) {
        return;
    }

    BOOST_CHECK_EQUAL(cursor.results.size(), expectations.ressize);

    for(size_t i = 0; i < cursor.results.size(); i++) {
        auto t = cursor.results[i].timestamp;
        auto value = get_pd_pointer<uint32_t>(cursor.results[i].data);
        if (direction == AKU_CURSOR_DIR_BACKWARD) {
            BOOST_CHECK_EQUAL(value[0], expectations.skew - i);
        } else {
            BOOST_CHECK_EQUAL(value[0], expectations.skew + i);
        }
        BOOST_CHECK_GE(t, begin);
        BOOST_CHECK_LE(t, end);
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
        aku_Timestamp inst = 1000L + i*time_skew;
        aku_MemRange range = {(void*)&i, sizeof(i)};
        BOOST_CHECK(page->add_entry(1, inst, range) != AKU_WRITE_STATUS_OVERFLOW);
    }
    return page;
}

void generic_search_test_with_skew
     ( int param_id
     , aku_Timestamp begin
     , aku_Timestamp end
     , int direction
     , ExpectedSearchResults const& expectations
     )
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 0x10000);
    auto page = init_search_range_test_with_skew(page_mem.data(), page_mem.size(), 1000, 2);

    SearchQuery query(param_id, begin, end, direction);
    RecordingCursor cursor;
    Caller caller;

    page->search(caller, &cursor, query);

    BOOST_CHECK_EQUAL(cursor.completed, expectations.completed);
    BOOST_CHECK_EQUAL(cursor.error_code, expectations.error_code);

    if (expectations.error_code != RecordingCursor::NO_ERROR)
        return;

    BOOST_CHECK_EQUAL(cursor.results.size(), expectations.ressize);

    std::vector<int64_t> timestamps;
    for(size_t i = 0; i < cursor.results.size(); i++) {
        auto ts = cursor.results[i].timestamp;
        BOOST_CHECK_GE(ts, begin);
        BOOST_CHECK_LE(ts, end);
        timestamps.push_back(ts);
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
    const int                   buf_len = 1024*1024*8;
    std::vector<char>           buffer(buf_len);
    std::vector<aku_Timestamp>  timestamps;
    std::vector<aku_ParamId>    paramids;
    aku_Timestamp               time_stamp = 0L;
    PageHeader*                 page = nullptr;

    page = new (&buffer[0]) PageHeader(0, buf_len, 0);

    auto max_timestamp = time_stamp;
    for(int i = 0; true; i++)
    {
        int rand_num = rand();
        aku_Timestamp inst = time_stamp;
        aku_ParamId id = 1 + (rand_num & 1);
        aku_MemRange range = {(void*)&i, sizeof(i)};
        if(page->add_entry(id, inst, range) == AKU_WRITE_STATUS_OVERFLOW) {
            break;
        }
        timestamps.push_back(time_stamp);  // i-th timestamp
        max_timestamp = std::max(time_stamp, max_timestamp);
        paramids.push_back(id);
        // timestamp grows always
        time_stamp += 1 + rand_num % 100;
    }

    for (int round = 0; round < 10; round++) {
        // select start timestamp
        int directions[] = {
            AKU_CURSOR_DIR_FORWARD,
            AKU_CURSOR_DIR_BACKWARD
        };
        int dir = directions[rand() & 1];
        double rnd_double = 0.001*((rand() + 1) % 200);
        aku_Timestamp start_time = (uint64_t)(rnd_double*max_timestamp);
        aku_Timestamp stop_time  = (uint64_t)((rnd_double + 0.6)*max_timestamp);
        aku_ParamId id2search = 1 + (rand() & 1);
        BOOST_REQUIRE(start_time > 0 && start_time < max_timestamp);
        BOOST_REQUIRE(stop_time > 0 && stop_time < max_timestamp);
        BOOST_REQUIRE(stop_time > start_time);
        SearchQuery query(id2search, start_time, stop_time, dir);
        Caller caller;
        RecordingCursor cursor;
        std::vector<uint32_t> matches;
        page->search(caller, &cursor, query);
        for(size_t i = 0; i < cursor.results.size(); i++) {
            const uint32_t* value = get_pd_pointer<uint32_t>(cursor.results.at(i).data);
            auto t = cursor.results.at(i).timestamp;
            auto id = cursor.results.at(i).param_id;
            auto index = value[0];
            matches.push_back(index);
            BOOST_REQUIRE_EQUAL(t, timestamps[index]);
            BOOST_REQUIRE(id == paramids[index]);
            BOOST_REQUIRE(value[0] == (uint32_t)index);
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


void generic_compression_test
    ( aku_ParamId param_id
    , aku_Timestamp begin
    , int dir
    , int n_elements_per_chunk
    )
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 0x10000);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0);

    ChunkHeader header;
    std::vector<ChunkHeader> expected;
    uint32_t pos = 0u;
    for (int i = 1; true; i++) {
        pos++;
        begin += 1 + std::rand() % 50;
        ChunkValue value;
        value.type = ChunkValue::BLOB;
        value.value.blobval.length = std::rand() % 10 + 1;
        value.value.blobval.offset = pos + std::rand() % 10;
        header.values.push_back(value);
        header.paramids.push_back(param_id);
        header.timestamps.push_back(begin);
        char buffer[100];
        aku_MemRange range = {buffer, static_cast<uint32_t>(std::rand() % 99 + 1)};
        uint32_t offset = 0u;
        auto status = page->add_chunk(range, header.paramids.size() * 33, &offset);
        if (status != AKU_SUCCESS) {
            break;
        }
        if (i % n_elements_per_chunk == 0) {
            status = page->complete_chunk(header);
            if (status != AKU_SUCCESS) {
                break;
            }
            // set expected
            expected.push_back(header);
            header = ChunkHeader();
        }
    }

    //page->_sort();

    BOOST_REQUIRE_NE(expected.size(), 0ul);

    // Test sequential access
    for(const auto& exp_chunk: expected) {
        auto ts_begin = exp_chunk.timestamps.front();
        auto ts_end = exp_chunk.timestamps.back();
        SearchQuery query(param_id, ts_begin, ts_end, dir);
        Caller caller;
        RecordingCursor cur;
        page->search(caller, &cur, query);

        BOOST_REQUIRE_EQUAL(cur.results.size(), exp_chunk.timestamps.size());

        if (dir == AKU_CURSOR_DIR_FORWARD) {
            auto act_it = cur.results.begin();
            for (auto i = 0ul; i != cur.results.size(); i++) {
                BOOST_REQUIRE_EQUAL(act_it->timestamp, exp_chunk.timestamps.at(i));
                BOOST_REQUIRE_EQUAL(act_it->param_id, exp_chunk.paramids.at(i));
                BOOST_REQUIRE_EQUAL(get_pd_length(act_it->data), exp_chunk.values.at(i).value.blobval.length);
                BOOST_REQUIRE_EQUAL(get_pd_pointer<void>(act_it->data), page->read_entry_data(exp_chunk.values.at(i).value.blobval.offset));
                act_it++;
            }
        } else {
            auto act_it = cur.results.rbegin();
            for (auto i = 0ul; i != cur.results.size(); i++) {
                BOOST_REQUIRE_EQUAL(act_it->timestamp, exp_chunk.timestamps[i]);
                BOOST_REQUIRE_EQUAL(act_it->param_id, exp_chunk.paramids[i]);
                BOOST_REQUIRE_EQUAL(get_pd_length(act_it->data), exp_chunk.values.at(i).value.blobval.length);
                BOOST_REQUIRE_EQUAL(get_pd_pointer<void>(act_it->data), page->read_entry_data(exp_chunk.values.at(i).value.blobval.offset));
                act_it++;
            }
        }
    }

    // Test random access
    for(const auto& exp_chunk: expected) {
        auto ix = std::rand() % (exp_chunk.timestamps.size() - 2);
        auto ts_begin = exp_chunk.timestamps[ix];
        auto ts_end = exp_chunk.timestamps[ix + 1];
        SearchQuery query(param_id, ts_begin, ts_end, dir);
        Caller caller;
        RecordingCursor cur;
        page->search(caller, &cur, query);

        BOOST_REQUIRE_EQUAL(cur.results.size(), 2u);
        if (dir == AKU_CURSOR_DIR_FORWARD) {
            BOOST_REQUIRE_EQUAL(cur.results[0].timestamp, ts_begin);
            BOOST_REQUIRE_EQUAL(cur.results[1].timestamp, ts_end);
        } else {
            BOOST_REQUIRE_EQUAL(cur.results[1].timestamp, ts_begin);
            BOOST_REQUIRE_EQUAL(cur.results[0].timestamp, ts_end);
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_Compression_forward_0) {
    generic_compression_test(1u, 0ul, AKU_CURSOR_DIR_FORWARD, 10);
}

BOOST_AUTO_TEST_CASE(Test_Compression_forward_1) {
    generic_compression_test(1u, 0ul, AKU_CURSOR_DIR_FORWARD, 100);
}

BOOST_AUTO_TEST_CASE(Test_Compression_backward_0) {
    generic_compression_test(1u, 0ul, AKU_CURSOR_DIR_BACKWARD, 10);
}

BOOST_AUTO_TEST_CASE(Test_Compression_backward_1) {
    generic_compression_test(1u, 0ul, AKU_CURSOR_DIR_BACKWARD, 100);
}
