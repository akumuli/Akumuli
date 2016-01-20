#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>
#include <thread>

#include "ingestion_pipeline.h"

using namespace Akumuli;

struct ConnectionMock : DbConnection {
    int cntp;
    int cntt;
    aku_Status write(const aku_Sample &sample) {
        if (sample.timestamp == 1) {
            cntt += 1;
            cntp += (int)sample.paramid;
        } else {
            BOOST_ERROR("Invalid value!");
        }
        return AKU_SUCCESS;
    }

    void close() {
    }

    std::string get_all_stats() {
        throw "not impelemnted";
    }

    std::shared_ptr<DbCursor> search(std::string query) {
        throw "not implemented";
    }

    int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
        throw "not implemented";
    }

    aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
        throw "not implemented";
    }
};

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
            aku_Sample sample = { 1ul, (aku_ParamId)i };
            spout->write(sample);
        }
        pipeline->stop();
        BOOST_REQUIRE_EQUAL(con->cntt, sumt);
        BOOST_REQUIRE_EQUAL(con->cntp, sump);
}
