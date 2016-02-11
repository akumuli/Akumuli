#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <apr.h>
#include <vector>
#include <iostream>

#include "sequencer.h"

using namespace Akumuli;
using namespace std;

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

}  // namespace

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_number_of_checkpoints) {

    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    aku_FineTuneParams params = {};
    params.window_size = SMALL_LOOP;
    Sequencer seq(params);

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_Timestamp>(i), 42u, 0));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock % 2 != 0) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;
        }
    }

    // one for data points that will be available after close
    num_checkpoints++;

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_busy_behavior)
{
    const int LOOP_SIZE = 10000;

    aku_FineTuneParams params = {};
    params.window_size = 10;
    Sequencer seq(params);

    int num_checkpoints = 0;

    for (int i = 0; i < LOOP_SIZE; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_Timestamp>(i), 42u, 0));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

        if (lock % 2 != 0) {
            // merge
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;
        }
    }

    // one for data points that will be available after close
    num_checkpoints++;

    BOOST_REQUIRE_EQUAL(num_checkpoints, LOOP_SIZE/10);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_order_of_elements)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    aku_FineTuneParams params = {};
    params.window_size = SMALL_LOOP;
    Sequencer seq(params);

    int num_checkpoints = 0;

    int begin = 0;
    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_Timestamp>(i), 42u, (double)i));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock % 2 == 1) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;

            // check order of the sorted run
            vector<aku_Sample> exp;
            int end = i - (SMALL_LOOP - 1);
            for (int j = begin; j != end; j++) {
                aku_Sample res;
                res.payload.type = AKU_PAYLOAD_FLOAT;
                res.payload.float64 = j;
                exp.emplace_back(res);
            }
            BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
            for(auto k = 0u; k < exp.size(); k++) {
                BOOST_REQUIRE_EQUAL(rec.results[k].payload.float64, exp[k].payload.float64);
            }
            begin = end;
        }
    }

    int lock = seq.reset();
    BOOST_REQUIRE(lock % 2 == 1);
    RecordingCursor rec;
    Caller caller;
    seq.merge(caller, &rec);
    num_checkpoints++;

    // check order of the sorted run
    vector<aku_Sample> exp;
    int end = LARGE_LOOP;
    for (int i = begin; i != end; i++) {
        aku_Sample res;
        res.payload.type = AKU_PAYLOAD_FLOAT;
        res.payload.float64 = i;
        exp.emplace_back(res);
    }
    BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
    for(auto k = 0u; k < exp.size(); k++) {
        BOOST_REQUIRE_EQUAL(rec.results[k].payload.float64, exp[k].payload.float64);
    }

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

struct Node : QP::Node {

    Caller& caller;
    RecordingCursor& cursor;

    Node(Caller& caller, RecordingCursor& cur) : caller(caller), cursor(cur) {}

    void complete() {
        cursor.complete(caller);
    }

    bool put(const aku_Sample &sample) {
        return sample.paramid == 42u ? cursor.put(caller, sample) : true;
    }

    void set_error(aku_Status status) {
        cursor.set_error(caller, status);
    }

    int get_requirements() const {
        return TERMINAL;
    }
};

struct TestQueryProcessor : QP::IQueryProcessor {

    // Search range
    aku_Timestamp begin;
    aku_Timestamp end;
    int dir;
    std::shared_ptr<QP::Node> root;

    TestQueryProcessor(std::shared_ptr<QP::Node> r, aku_Timestamp b, aku_Timestamp e, int dir)
        : begin(b)
        , end(e)
        , dir(dir)
        , root(r)
    {
    }

    //! Lowerbound
    virtual aku_Timestamp lowerbound() const { return begin; }

    //! Upperbound
    virtual aku_Timestamp upperbound() const { return end; }

    //! Scan direction (AKU_CURSOR_DIR_BACKWARD or AKU_CURSOR_DIR_FORWARD)
    virtual int direction() const { return dir; }

    virtual QP::QueryRange range() const {
        return { begin, end, dir, QP::QueryRange::INSTANT };
    }

    virtual QP::IQueryFilter& filter() {
        static QP::BypassFilter bypass;
        return bypass;
    }

    virtual SeriesMatcher* matcher() {
        return nullptr;
    }

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


void test_sequencer_searching(int dir) {
    const int SZLOOP = 1000;
    const int WINDOW = 10000;

    aku_FineTuneParams params = {};
    params.window_size = WINDOW;
    Sequencer seq(params);
    std::vector<double> expected;

    for (int i = 0; i < SZLOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_Timestamp>(42u + i), 42u, i));
        expected.push_back(i);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        BOOST_REQUIRE(lock % 2 == 0);  // because window is larger than number of iterations
    }

    aku_Timestamp begin = AKU_MIN_TIMESTAMP,
                  end   = AKU_MAX_TIMESTAMP;

    if (dir == AKU_CURSOR_DIR_BACKWARD) {
        std::reverse(expected.begin(), expected.end());
    }

    Caller caller;
    RecordingCursor cursor;
    auto node = std::make_shared<Node>(caller, cursor);
    auto qproc = std::make_shared<TestQueryProcessor>(node, begin, end, dir);

    aku_Timestamp window;
    int seq_id;
    std::tie(window, seq_id) = seq.get_window();
    seq.search(qproc, seq_id);

    // Check that everything is there
    BOOST_REQUIRE_EQUAL(cursor.results.size(), expected.size());
    for (auto i = 0u; i < cursor.results.size(); i++) {
        auto actual = cursor.results[i].payload.float64;
        BOOST_REQUIRE_EQUAL(actual, expected[i]);
    }
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_backward) {
    test_sequencer_searching(AKU_CURSOR_DIR_BACKWARD);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_forward) {
    test_sequencer_searching(AKU_CURSOR_DIR_FORWARD);
}
