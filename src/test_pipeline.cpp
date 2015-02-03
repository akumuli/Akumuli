#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>
#include <thread>

#include "ingestion_pipeline.h"

struct ConnectionMock : Akumuli::DbConnection {
    int cntp;
    int cntt;
    aku_Status write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        if (ts == 1) {
            cntt += 1;
            cntp += (int)param;
        } else {
            BOOST_ERROR("Invalid value!");
        }
        return AKU_SUCCESS;
    }
};

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_spout_in_single_thread) {

        std::shared_ptr<ConnectionMock> con = std::make_shared<ConnectionMock>();
        con->cntp = 0;
        con->cntt = 0;
        auto pipeline = std::make_shared<IngestionPipeline>(con, AKU_LINEAR_BACKOFF);
        pipeline->start();
        auto spout = pipeline->make_spout();
        int sump = 0;
        int sumt = 0;
        for (int i = 0; i < 10000; i++) {
            sump += i;
            sumt += 1;
            spout->write_double(i, 1, 0.0);
        }
        pipeline->stop();
        BOOST_REQUIRE_EQUAL(con->cntt, sumt);
        BOOST_REQUIRE_EQUAL(con->cntp, sump);
}
