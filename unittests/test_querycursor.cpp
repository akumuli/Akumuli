// tests for akumulid/query_cursor.cpp

#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>
#include <thread>

#include "query_results_pooler.h"

using namespace Akumuli;

struct CursorMock : DbCursor {

    constexpr static const double floatval = 3.1415;
    bool isdone_ = false;

    size_t read(void *dest, size_t dest_size) {
        return read_impl((aku_Sample*)dest, dest_size);
    }

    size_t read_impl(aku_Sample *dest, size_t dest_size) {
        if (isdone_ == true) {
            return 0;
        }
        if (dest_size < 2*sizeof(aku_Sample)) {
            BOOST_FAIL("invalid mock usage");
        }
        // first value
        dest[0].paramid = 33;
        aku_parse_timestamp("20141210T074243.111999", &dest[0]);
        dest[0].payload.size = sizeof(aku_Sample);
        dest[0].payload.type = AKU_PAYLOAD_FLOAT;
        dest[0].payload.float64 = floatval;

        // second value
        dest[1].paramid = 44;
        aku_parse_timestamp("20141210T122434.999111", &dest[1]);
        dest[1].payload.size = sizeof(aku_Sample);
        dest[1].payload.type = AKU_PAYLOAD_FLOAT;
        dest[1].payload.float64 = floatval;

        isdone_ = true;
        return 2*sizeof(aku_Sample);
    }

    int is_done() {
        return isdone_;
    }

    bool is_error(aku_Status *out_error_code_or_null) {
        if (out_error_code_or_null) {
            *out_error_code_or_null = AKU_SUCCESS;
        }
        return false;
    }

    void close() {}
};

struct SessionMock : DbSession {

    aku_Status write(const aku_Sample &sample) {
        return AKU_SUCCESS;
    }

    void close() {
    }

    std::shared_ptr<DbCursor> query(std::string query) {
        return std::make_shared<CursorMock>();
    }

    std::shared_ptr<DbCursor> suggest(std::string query) {
        return std::make_shared<CursorMock>();
    }

    std::shared_ptr<DbCursor> search(std::string query) {
        return std::make_shared<CursorMock>();
    }

    int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
        std::string strid = std::to_string(id);
        if (strid.size() < buffer_size) {
            memcpy(buffer, strid.data(), strid.size());
            return static_cast<int>(strid.size());
        }
        return -1*static_cast<int>(strid.size());
    }

    virtual int name_to_param_id_list(const char* begin, const char* end, aku_ParamId* ids, u32 cap) override {
        throw "not implemented";
    }

    aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
        throw "not implemented";
    }
};

struct ConnectionMock : DbConnection
{
    virtual std::string get_all_stats() override {
        return "{}";
    }

    virtual std::shared_ptr<DbSession> create_session() override {
        return std::make_shared<SessionMock>();
    }
};

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_query_cursor) {

    std::string expected = "+33\r\n+20141210T074243.111999000\r\n+3.1415000000000002\r\n+44\r\n+20141210T122434.999111000\r\n+3.1415000000000002\r\n";
    std::shared_ptr<DbSession> session;
    session.reset(new SessionMock());
    char buffer[0x1000];
    QueryResultsPooler cursor(session, 1000, ApiEndpoint::QUERY);
    cursor.append("{}", 2);
    cursor.start();
    size_t len;
    bool done;
    std::tie(len, done) = cursor.read_some(buffer, 0x1000);
    BOOST_REQUIRE(len > 0);
    BOOST_REQUIRE(!done);
    auto actual = std::string(buffer, buffer + len);
    BOOST_REQUIRE_EQUAL(expected, actual);
}
