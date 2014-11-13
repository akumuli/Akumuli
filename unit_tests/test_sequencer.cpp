#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "sequencer.h"
#include "cursor.h"

using namespace Akumuli;
using namespace std;

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

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_number_of_checkpoints)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {0u, SMALL_LOOP, 0u});

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, 0u, 0u));
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
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {0u, SMALL_LOOP, 0u});

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, 0u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock % 2 != 0) {
            // present write (ts <= last checkpoint)
            for (int j = 0; j < SMALL_LOOP; j++) {
                int other_lock = 0;
                tie(status, other_lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i + j), 24u, 0u, 0u));
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
                BOOST_REQUIRE_EQUAL(other_lock % 2, 0);
            }

            // future write (ts > last checkpoint)
            int other_lock = 0;
            tie(status, other_lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i + SMALL_LOOP), 24u, 0u, 0u));
            BOOST_REQUIRE_EQUAL(status, AKU_EBUSY);
            BOOST_REQUIRE_EQUAL(other_lock % 2, 0);

            // merge
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

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_order_of_elements)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {0u, SMALL_LOOP, 0u});

    int num_checkpoints = 0;

    int begin = 0;
    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, i, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock % 2 == 1) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;

            // check order of the sorted run
            vector<CursorResult> exp;
            int end = i - (SMALL_LOOP - 1);
            for (int j = begin; j != end; j++) {
                CursorResult res;
                res.data = reinterpret_cast<void*>(j);
                exp.emplace_back(res);
            }
            BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
            for(auto k = 0u; k < exp.size(); k++) {
                BOOST_REQUIRE_EQUAL(rec.results[k].data, exp[k].data);
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
    vector<CursorResult> exp;
    int end = LARGE_LOOP;
    for (int i = begin; i != end; i++) {
        CursorResult res;
        res.data = reinterpret_cast<void*>(i);  // NOTE: this is a hack, page in sequencer is null but merge
        exp.emplace_back(res);                  // wouldn't fail, it will return offset value as pointer
    }                                           // because it will be added to `this` and `this` == null.
    BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
    for(auto k = 0u; k < exp.size(); k++) {
        BOOST_REQUIRE_EQUAL(rec.results[k].data, exp[k].data);
    }

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

void test_sequencer_searching(int dir) {
    const int SZLOOP = 1000;
    const int WINDOW = 10000;

    Sequencer seq(nullptr, {0u, WINDOW, 0u});
    std::vector<aku_EntryOffset> offsets;

    for (int i = 0; i < SZLOOP; i++) {
        int status;
        int lock = 0;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(42u + i), 42u, i, 0u));
        offsets.push_back(i);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        BOOST_REQUIRE(lock % 2 == 0);  // because window is larger than number of iterations
    }

    aku_TimeStamp begin = AKU_MIN_TIMESTAMP,
                  end   = AKU_MAX_TIMESTAMP;

    if (dir == AKU_CURSOR_DIR_BACKWARD) {
        std::reverse(offsets.begin(), offsets.end());
    }

    Caller caller;
    RecordingCursor cursor;
    SearchQuery query(42u, begin, end, dir);
    aku_TimeStamp window;
    int seq_id;
    std::tie(window, seq_id) = seq.get_window();
    seq.search(caller, &cursor, query, seq_id);

    // Check that everything is there
    BOOST_REQUIRE_EQUAL(cursor.results.size(), offsets.size());
    for (auto i = 0u; i < cursor.results.size(); i++) {
        auto offset = reinterpret_cast<uint64_t>(cursor.results[i].data);
        BOOST_REQUIRE_EQUAL(offset, offsets[i]);
    }
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_backward) {
    test_sequencer_searching(AKU_CURSOR_DIR_BACKWARD);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_forward) {
    test_sequencer_searching(AKU_CURSOR_DIR_FORWARD);
}
