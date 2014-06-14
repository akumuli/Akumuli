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

    for (int i = 0; i <= LARGE_LOOP; i++) {
        int status;
        bool cp;
        tie(status, cp) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (cp) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;
        }
    }

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_busy_behavior)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    for (int i = 0; i <= LARGE_LOOP; i++) {
        int status;
        bool cp;
        tie(status, cp) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, 0u));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (cp) {
            // present write (ts <= last checkpoint)
            for (int j = 0; j < SMALL_LOOP; j++) {
                tie(status, cp) = seq.add(TimeSeriesValue(TimeStamp::make(i + j), 24u, 0u));
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
                BOOST_REQUIRE_EQUAL(cp, false);
            }

            // future write (ts > last checkpoint)
            tie(status, cp) = seq.add(TimeSeriesValue(TimeStamp::make(i + SMALL_LOOP), 24u, 0u));
            BOOST_REQUIRE_EQUAL(status, AKU_EBUSY);
            BOOST_REQUIRE_EQUAL(cp, false);

            // merge
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
            num_checkpoints++;
        }
    }

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}

BOOST_AUTO_TEST_CASE(Test_sequencer_correct_order_of_elements)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    int begin = 0;
    for (int i = 0; i <= LARGE_LOOP; i++) {
        int status;
        bool cp;
        tie(status, cp) = seq.add(TimeSeriesValue(TimeStamp::make(i), 42u, i));
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        if (cp) {
            RecordingCursor rec;
            Caller caller;
            seq.merge(caller, &rec);
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

    // TODO: check last elements

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP);
}
