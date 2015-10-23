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
    std::vector<aku_Sample> results;
    bool completed = false;
    enum ErrorCodes {
        NO_ERROR = -1
    };
    int error_code = NO_ERROR;

    virtual bool put(Caller&, aku_Sample const& result) {
        results.push_back(result);
        return true;
    }

    virtual void complete(Caller&) {
        completed = true;
    }

    virtual void set_error(Caller&, aku_Status error_code) {
        this->error_code = (int)error_code;
    }
};

struct Recorder : QP::Node {

    Caller caller;
    RecordingCursor cursor;
    aku_ParamId expected_id;

    Recorder(aku_ParamId expected) : expected_id(expected) {}

    void complete() {
        cursor.complete(caller);
    }

    bool put(const aku_Sample &sample) {
        return sample.paramid == expected_id ? cursor.put(caller, sample) : true;
    }

    void set_error(aku_Status status) {
        cursor.set_error(caller, status);
    }

    int get_requirements() const {
        return EMPTY;
    }
};

struct TestQueryProcessor : QP::IQueryProcessor {

    // Search range
    aku_Timestamp begin;
    aku_Timestamp end;
    int dir;
    std::shared_ptr<QP::Node> root;

    TestQueryProcessor(aku_Timestamp b, aku_Timestamp e, int d, std::shared_ptr<QP::Node> r)
        : begin(b)
        , end(e)
        , dir(d)
        , root(r)
    {
    }

    //! Lowerbound
    virtual aku_Timestamp lowerbound() const { return begin; }

    //! Upperbound
    virtual aku_Timestamp upperbound() const { return end; }

    virtual const QP::IQueryFilter& filter() const {
        static QP::BypassFilter bypass;
        return bypass;
    }

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    virtual int direction() const { return dir; }

    /** Will be called before query execution starts.
      * If result already obtained - return False.
      * In this case `stop` method shouldn't be called
      * at the end.
      */
    virtual bool start() { return true; }

    //! Get new value
    virtual bool put(const aku_Sample& sample) {
        return root->put(sample);
    }

    //! Will be called when processing completed without errors
    virtual void stop() {
        root->complete();
    }

    //! Will be called on error
    virtual void set_error(aku_Status error) {
        root->set_error(error);
    }

};

// Make query processor
std::shared_ptr<QP::IQueryProcessor> make_proc(std::shared_ptr<QP::Node> root, aku_Timestamp begin, aku_Timestamp end, int dir) {
    return std::make_shared<TestQueryProcessor>(begin, end, dir, root);
}

}  // namespace

AkumuliInitializer initializer;

BOOST_AUTO_TEST_CASE(TestPaging1) {

    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    BOOST_CHECK_EQUAL(0, page->get_entries_count());
    BOOST_CHECK_EQUAL(4096, page->get_free_space());
}

BOOST_AUTO_TEST_CASE(TestPaging2) {

    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    auto free_space_before = page->get_free_space();
    char buffer[128];
    aku_MemRange range = {buffer, 128};
    auto result = page->add_entry(1, 2, range);
    BOOST_CHECK_EQUAL(result, AKU_SUCCESS);
    auto free_space_after = page->get_free_space();
    BOOST_CHECK_EQUAL(free_space_before - free_space_after, sizeof(aku_Entry) + 128 + sizeof(aku_EntryIndexRecord));
}

BOOST_AUTO_TEST_CASE(TestPaging3)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    aku_MemRange range = {nullptr, static_cast<uint32_t>(page_mem.size())};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_EOVERFLOW);
}

BOOST_AUTO_TEST_CASE(TestPaging4)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    aku_MemRange range = {nullptr, 0};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(TestPaging5)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    char buffer[222];
    aku_MemRange range = {buffer, 222};
    auto result = page->add_entry(0, 1, range);
    BOOST_CHECK_EQUAL(result, AKU_SUCCESS);
    auto len = page->get_entry_length_at(0);
    BOOST_CHECK_EQUAL(len, 222);
}

BOOST_AUTO_TEST_CASE(TestPaging6)
{
    std::vector<char> page_mem;
    page_mem.resize(sizeof(PageHeader) + 4096);
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    uint32_t buffer[] = {0, 1, 2, 3, 4, 5, 6, 7};
    aku_Timestamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(buffer)};
    auto result = page->add_entry(3333, inst, range);
    BOOST_CHECK_EQUAL(result, AKU_SUCCESS);

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
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);
    uint32_t buffer[] = {1, 2, 3, 4};
    aku_Timestamp inst = 1111L;
    aku_MemRange range = {(void*)buffer, sizeof(buffer)};
    auto result = page->add_entry(3333, inst, range);

    BOOST_CHECK_EQUAL(result, AKU_SUCCESS);

    auto centry = page->read_entry_at(0);
    BOOST_CHECK_EQUAL(centry->length, range.length);
    BOOST_CHECK_EQUAL(centry->param_id, 3333);
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
    auto page = new (page_mem.data()) PageHeader(0, page_mem.size(), 0, 1);

    UncompressedChunk header;
    std::vector<UncompressedChunk> expected;
    uint32_t pos = 0u;
    for (int i = 1; true; i++) {
        pos++;
        begin += 1 + std::rand() % 50;
        double value = pos + std::rand() % 10;
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
            header = UncompressedChunk();
        }
    }

    BOOST_REQUIRE_NE(expected.size(), 0ul);

    // Test sequential access
    for(const auto& exp_chunk: expected) {
        auto ts_begin = exp_chunk.timestamps.front();
        auto ts_end = exp_chunk.timestamps.back();

        auto recorder = std::make_shared<Recorder>(param_id);
        auto qproc = make_proc(recorder, ts_begin, ts_end, dir);

        page->search(qproc);

        auto cur = recorder->cursor;

        BOOST_REQUIRE_EQUAL(cur.results.size(), exp_chunk.timestamps.size());

        if (dir == AKU_CURSOR_DIR_FORWARD) {
            auto act_it = cur.results.begin();
            for (auto i = 0ul; i != cur.results.size(); i++) {
                BOOST_REQUIRE_EQUAL(act_it->timestamp, exp_chunk.timestamps.at(i));
                BOOST_REQUIRE_EQUAL(act_it->paramid, exp_chunk.paramids.at(i));
                BOOST_REQUIRE_EQUAL(act_it->payload.float64, exp_chunk.values.at(i));
                act_it++;
            }
        } else {
            auto act_it = cur.results.rbegin();
            for (auto i = 0ul; i != cur.results.size(); i++) {
                BOOST_REQUIRE_EQUAL(act_it->timestamp, exp_chunk.timestamps[i]);
                BOOST_REQUIRE_EQUAL(act_it->paramid, exp_chunk.paramids[i]);
                BOOST_REQUIRE_EQUAL(act_it->payload.float64, exp_chunk.values.at(i));
                act_it++;
            }
        }
    }

    // Test random access
    for(const auto& exp_chunk: expected) {
        auto ix = std::rand() % (exp_chunk.timestamps.size() - 2);
        auto ts_begin = exp_chunk.timestamps[ix];
        auto ts_end = exp_chunk.timestamps[ix + 1];

        auto recorder = std::make_shared<Recorder>(param_id);
        auto qproc = make_proc(recorder, ts_begin, ts_end, dir);

        page->search(qproc);

        auto cur = recorder->cursor;

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
