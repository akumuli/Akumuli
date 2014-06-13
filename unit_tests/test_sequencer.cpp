#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <iostream>
#include <boost/test/unit_test.hpp>

#include "cache.h"
#include "cursor.h"

using namespace Akumuli;
using namespace std;

BOOST_AUTO_TEST_CASE(Test_sequencer_0)
{
    const int LARGE_LOOP = 1000;
    const int SMALL_LOOP = 10;

    Sequencer seq(nullptr, {SMALL_LOOP});

    int num_checkpoints = 0;

    for (int i = 0; i < LARGE_LOOP; i++) {
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

    BOOST_REQUIRE_EQUAL(num_checkpoints, LARGE_LOOP/SMALL_LOOP - 1);
}
