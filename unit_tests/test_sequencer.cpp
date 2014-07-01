#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"
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
        tie(status, lock) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, 0u));
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
        tie(status, lock) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (lock.owns_lock()) {
            // present write (ts <= last checkpoint)
            for (int j = 0; j < SMALL_LOOP; j++) {
                Sequencer::Lock other_lock;
                tie(status, other_lock) = seq.add(TimeSeriesValue(TimeStamp::make(i + j), 24u, 0u));
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
                BOOST_REQUIRE_EQUAL(other_lock.owns_lock(), false);
            }

            // future write (ts > last checkpoint)
            Sequencer::Lock other_lock;
            tie(status, other_lock) = seq.add(TimeSeriesValue(TimeStamp::make(i + SMALL_LOOP), 24u, 0u));
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
        tie(status, lock) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, i));
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
                exp.emplace_back(j, nullptr);
            }
            BOOST_REQUIRE_EQUAL(rec.offsets.size(), exp.size());
            BOOST_REQUIRE_EQUAL_COLLECTIONS(rec.offsets.begin(), rec.offsets.end(), exp.begin(), exp.end());
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
        exp.emplace_back(i, nullptr);
    }
    BOOST_REQUIRE_EQUAL(rec.offsets.size(), exp.size());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(rec.offsets.begin(), rec.offsets.end(), exp.begin(), exp.end());

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_searching) {
    const int SZLOOP = 1000;
    const int WINDOW = 10000;

    Sequencer seq(nullptr, {WINDOW});
    std::vector<EntryOffset> offsets;

    for (int i = 0; i < SZLOOP; i++) {
        int status;
        Sequencer::Lock lock;
        tie(status, lock) = seq.add(TimeSeriesValue(TimeStamp::make(42u + i), 42u, i));
        offsets.push_back(i);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        BOOST_REQUIRE(!lock.owns_lock());  // because window is larger than number of iterations
    }

    Caller caller;
    RecordingCursor cursor;
    SearchQuery query(42u, TimeStamp::MIN_TIMESTAMP, TimeStamp::MAX_TIMESTAMP, AKU_CURSOR_DIR_FORWARD);
    seq.search(caller, &cursor, query);

    // Check that everything is there
    BOOST_REQUIRE_EQUAL(cursor.offsets.size(), offsets.size());
    for (auto i = 0u; i < cursor.offsets.size(); i++) {
        auto offset = cursor.offsets[i].first;
        BOOST_REQUIRE_EQUAL(offset, offsets[i]);
    }
}
