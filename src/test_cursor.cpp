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
    std::vector<CursorResult> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (aku_EntryOffset i = 0u; i < (aku_EntryOffset)n_iter; i++) {
            CursorResult r;
            r.data = reinterpret_cast<void*>(i);
            r.length = sizeof(i);
            cursor.put(caller, r);
            expected.push_back(r);
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
        BOOST_REQUIRE_EQUAL(expected.at(i).data, actual.at(i).data);
    }
}

void test_cursor_error(int n_iter, int buf_size) {
    CoroCursor cursor;
    std::vector<CursorResult> expected;
    auto generator = [n_iter, &expected, &cursor](Caller& caller) {
        for (aku_EntryOffset i = 0u; i < (aku_EntryOffset)n_iter; i++) {
            CursorResult r;
            r.data = reinterpret_cast<void*>(i);
            r.length = sizeof(i);
            cursor.put(caller, r);
            expected.push_back(r);
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
        BOOST_REQUIRE_EQUAL(expected.at(i).data, actual.at(i).data);
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

    PageWrapper(int page_size, uint32_t id) {
        count = 0;
        page_id = id;
        buf = new char[page_size];
        page = new (buf) PageHeader(0, page_size, (int)page_id);
        init();
    }

    ~PageWrapper() {
        delete buf;
    }

    void init() {
        while(true) {
            aku_MemRange load = {(void*)&page_id, sizeof(page_id)};
            auto status = page->add_entry(rand() % 100, rand(), load);
            if (status != AKU_SUCCESS) {
                break;
            }
            count++;
        }
        page->_sort();
        std::vector<int64_t> timestamps;
        for (auto ix = 0u; ix < page->count; ix++) {
            auto entry = page->read_entry_at(ix);
            timestamps.push_back(entry->time);
        }
        SortPred pred = { AKU_CURSOR_DIR_FORWARD };
        pred.check_order(timestamps.begin(), timestamps.end());
    }
};

template<class FanInCursor>
void test_fan_in_cursor(uint32_t dir, int n_cursors, int page_size) {
    std::vector<PageWrapper> pages;
    pages.reserve(n_cursors);
    for (int i = 0; i < n_cursors; i++) {
        pages.emplace_back(page_size, (uint32_t)i);
    }

    auto match_all = [](aku_ParamId) { return SearchQuery::MATCH; };
    SearchQuery q(match_all, AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, dir);

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

    FanInCursor cursor(&ecur[0], n_cursors, (int)dir);

    CursorResult results[0x100];
    int count = 0;
    std::vector<int64_t> actual_results;  // must be sorted
    while(!cursor.is_done()) {
        int n_read = cursor.read(results, 0x100);
        count += n_read;
        for (int i = 0; i < n_read; i++) {
            actual_results.push_back(results[i].timestamp);
        }
    }
    cursor.close();

    std::vector<int64_t> expected_results;
    for(auto& pagewrapper: pages) {
        PageHeader* page = pagewrapper.page;
        for (auto i = 0u; i < pagewrapper.count; i++) {
            const aku_Entry* entry = page->read_entry_at(i);
            expected_results.push_back(entry->time);
        }
    }
    SortPred s = {dir};
    std::sort(expected_results.begin(), expected_results.end(), s);
    s.check_order(actual_results.begin(), actual_results.end());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual_results.begin(), actual_results.end(), expected_results.begin(), expected_results.end());
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_1_f)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 1, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_2_f)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 10, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_3_f)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 1, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_4_f)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_FORWARD, 10, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_1_b)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 1, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_2_b)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 10, 1000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_3_b)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 1, 100000 + sizeof(PageHeader));
}

BOOST_AUTO_TEST_CASE(Test_fan_in_cursor_4_b)
{
    test_fan_in_cursor<FanInCursorCombinator>(AKU_CURSOR_DIR_BACKWARD, 10, 100000 + sizeof(PageHeader));
}

// Stackless fan-in cursor

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

struct CompressedPageWrapper {
    char* buf;
    PageHeader* page;
    uint32_t page_id;
    size_t count;
    aku_TimeStamp max_ts, min_ts;

    CompressedPageWrapper(int page_size, uint32_t id) {
        min_ts = AKU_MAX_TIMESTAMP;
        max_ts = AKU_MIN_TIMESTAMP;
        count = 0;
        page_id = id;
        buf = new char[page_size];
        page = new (buf) PageHeader(0, page_size, (int)page_id);
        init();
    }

    ~CompressedPageWrapper() {
        delete buf;
    }

private:
    void init() {
        std::vector<aku_TimeStamp> timestamps;
        std::vector<aku_ParamId> params;
        std::vector<uint32_t> lengths, offsets;
        aku_TimeStamp ts = 0u;
        aku_ParamId  pid = page_id;
        while(true) {
            aku_MemRange load = {(void*)&page_id, sizeof(page_id)};
            auto space_est = static_cast<uint32_t>((sizeof(aku_TimeStamp)+2*sizeof(uint32_t))*timestamps.size());
            auto status = page->add_chunk(load, space_est);
            if (status != AKU_SUCCESS) {
                break;
            }
            offsets.push_back(page->last_offset);
            timestamps.push_back(ts);
            params.push_back(pid);
            lengths.push_back(sizeof(page_id));
            min_ts = std::min(ts, min_ts);
            max_ts = std::max(ts, max_ts);
            ts++;
        }
        ChunkHeader header;
        header.timestamps.swap(timestamps);
        header.paramids.swap(params);
        header.lengths.swap(lengths);
        header.offsets.swap(offsets);
        auto status = page->complete_chunk(header);
        if (status != AKU_SUCCESS) {
            BOOST_ERROR("Can't complete chunk");
        }
    }
};

void test_chunk_cursor(bool backward, bool do_binary_search) {
    CompressedPageWrapper wpage(0x1000+sizeof(PageHeader), 42);
    const aku_Entry *entry = nullptr;
    if (backward) {
        entry = wpage.page->read_entry_at(0);
        if (entry->param_id != AKU_CHUNK_BWD_ID) {
            BOOST_ERROR("Invalid chunks order for backward search");
        }
    } else {
        entry = wpage.page->read_entry_at(1);
        if (entry->param_id != AKU_CHUNK_FWD_ID) {
            BOOST_ERROR("Invalid chunks order for forward search");
        }
    }
    auto scan_dir = backward ? AKU_CURSOR_DIR_BACKWARD : AKU_CURSOR_DIR_FORWARD;
    SearchQuery squery(42, wpage.min_ts, wpage.max_ts, scan_dir);
    ChunkCursor cursor(wpage.page, entry, wpage.page->count/2, squery, backward, do_binary_search);
    aku_TimeStamp expected_ts = backward ? wpage.max_ts : wpage.min_ts;
    while(!cursor.is_done()) {
        const auto NRES = 32;
        CursorResult results[NRES];
        const auto NREAD = cursor.read(results, NRES);
        for (int i = 0; i < NREAD; i++) {
            const auto value = results[i];
            if (value.length != sizeof(uint32_t)) {
                BOOST_MESSAGE("Invalid length, " << value.length << " at " << i);
                BOOST_REQUIRE(value.length == sizeof(uint32_t));
            }
            if (value.param_id != 42) {
                BOOST_MESSAGE("Invalid param_id, " << value.param_id << " at " << i);
                BOOST_REQUIRE(value.param_id == 42);
            }
            if (value.timestamp != expected_ts) {
                BOOST_MESSAGE("Invalid timestamp, " << value.timestamp << " at " << i);
                BOOST_REQUIRE(value.timestamp == expected_ts);
            }
            auto content = *reinterpret_cast<const uint32_t*>(value.data);
            if (content != 42) {
                BOOST_MESSAGE("Invalid content, " << content << " at " << i);
                BOOST_REQUIRE(*reinterpret_cast<const uint32_t*>(value.data) == 42);
            }
            if (backward) {
                expected_ts--;
            } else {
                expected_ts++;
            }
        }
    }
    cursor.close();
}

//! Check forward ChunkCursor without binary search
BOOST_AUTO_TEST_CASE(Test_chunk_cursor_fw_nobs)
{
    test_chunk_cursor(false, false);
}

//! Check forward ChunkCursor with binary search
BOOST_AUTO_TEST_CASE(Test_chunk_cursor_fw_bs)
{
    test_chunk_cursor(false, true);
}

//! Check backward ChunkCursor without binary search
BOOST_AUTO_TEST_CASE(Test_chunk_cursor_bw_nobs)
{
    test_chunk_cursor(true, false);
}

//! Check backward ChunkCursor with binary search
BOOST_AUTO_TEST_CASE(Test_chunk_cursor_bw_bs)
{
    test_chunk_cursor(true, true);
}
