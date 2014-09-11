#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "sequencer.h"
#include "cursor.h"

using namespace Akumuli;
using namespace std;


BOOST_AUTO_TEST_CASE(Test_sequencer_correct_number_of_checkpoints)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        Sequencer::Lock lock;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, 0u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock.owns_lock()) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec, std::move(lock));
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

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        Sequencer::Lock lock;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, 0u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock.owns_lock()) {
            // present write (ts <= last checkpoint)
            for (int j = 0; j < SMALL_LOOP; j++) {
                Sequencer::Lock other_lock;
                tie(status, other_lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i + j), 24u, 0u, 0u));
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
                BOOST_REQUIRE_EQUAL(other_lock.owns_lock(), false);
            }

            // future write (ts > last checkpoint)
            Sequencer::Lock other_lock;
            tie(status, other_lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i + SMALL_LOOP), 24u, 0u, 0u));
            BOOST_REQUIRE_EQUAL(status, AKU_EBUSY);
            BOOST_REQUIRE_EQUAL(other_lock.owns_lock(), false);

            // merge
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec, std::move(lock));
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

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    int begin = 0;
    for (int i = 0; i < LARGE_LOOP; i++) {
        int status;
        Sequencer::Lock lock;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(i), 42u, i, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock.owns_lock()) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec, std::move(lock));
            num_checkpoints++;

            // check order of the sorted run
            vector<CursorResult> exp;
            int end = i - (SMALL_LOOP - 1);
            for (int j = begin; j != end; j++) {
                CursorResult res;
                res.offset = j;
                exp.emplace_back(res);
            }
            BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
            // BOOST_REQUIRE_EQUAL_COLLECTIONS(rec.results.begin(), rec.results.end(), exp.begin(), exp.end());
            for(auto k = 0u; k < exp.size(); k++) {
                BOOST_REQUIRE_EQUAL(rec.results[k].offset, exp[k].offset);
            }
            begin = end;
        }
    }

    Sequencer::Lock lock = seq.close();
    BOOST_REQUIRE(lock.owns_lock());
    RecordingCursor rec;
    Caller caller;
    seq.merge(caller, &rec, std::move(lock));
    num_checkpoints++;

    // check order of the sorted run
    vector<CursorResult> exp;
    int end = LARGE_LOOP;
    for (int i = begin; i != end; i++) {
        CursorResult res;
        res.offset = i;
        exp.emplace_back(res);
    }
    BOOST_REQUIRE_EQUAL(rec.results.size(), exp.size());
    // BOOST_REQUIRE_EQUAL_COLLECTIONS(rec.results.begin(), rec.results.end(), exp.begin(), exp.end());
    for(auto k = 0u; k < exp.size(); k++) {
        BOOST_REQUIRE_EQUAL(rec.results[k].offset, exp[k].offset);
    }

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

void test_sequencer_searching(int dir) {
    const int SZLOOP = 1000;
    const int WINDOW = 10000;

    Sequencer seq(nullptr, {WINDOW});
    std::vector<aku_EntryOffset> offsets;

    for (int i = 0; i < SZLOOP; i++) {
        int status;
        Sequencer::Lock lock;
        tie(status, lock) = seq.add(TimeSeriesValue(static_cast<aku_TimeStamp>(42u + i), 42u, i, 0u));
        offsets.push_back(i);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        BOOST_REQUIRE(!lock.owns_lock());  // because window is larger than number of iterations
    }

    aku_TimeStamp begin = AKU_MIN_TIMESTAMP,
                  end   = AKU_MAX_TIMESTAMP;

    if (dir == AKU_CURSOR_DIR_BACKWARD) {
        std::reverse(offsets.begin(), offsets.end());
    }

    Caller caller;
    RecordingCursor cursor;
    SearchQuery query(42u, begin, end, dir);
    seq.search(caller, &cursor, query);

    // Check that everything is there
    BOOST_REQUIRE_EQUAL(cursor.results.size(), offsets.size());
    for (auto i = 0u; i < cursor.results.size(); i++) {
        auto offset = cursor.results[i].offset;
        BOOST_REQUIRE_EQUAL(offset, offsets[i]);
    }
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_backward) {
    test_sequencer_searching(AKU_CURSOR_DIR_BACKWARD);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_search_forward) {
    test_sequencer_searching(AKU_CURSOR_DIR_FORWARD);
}
