// tests for akumulid/query_cursor.cpp

#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>
#include <thread>

#include "query_cursor.h"

using namespace Akumuli;

struct CursorMock : DbCursor {

    constexpr static const double floatval = 3.1415;
    constexpr static const char* strval = "teststring";
    bool isdone_ = false;

    int read(aku_Sample *dest, size_t dest_size) {
        if (isdone_ == true) {
            return 0;
        }
        if (dest_size < 2) {
            BOOST_FAIL("invalid mock usage");
        }
        // first value
        dest[0].paramid = 33;
        aku_parse_timestamp("20141210T074243.111999", &dest[0]);
        dest[0].payload.type = aku_PData::FLOAT;
        dest[0].payload.value.float64 = floatval;

        // second value
        dest[1].paramid = 44;
        aku_parse_timestamp("20141210T122434.999111", &dest[1]);
        dest[1].payload.type = aku_PData::BLOB;
        dest[1].payload.value.blob.begin = strval;
        dest[1].payload.value.blob.size = strlen(strval);

        isdone_ = true;
        return 2;
    }

    int is_done() {
        return isdone_;
    }

    bool is_error(int *out_error_code_or_null) {
        if (out_error_code_or_null) {
            *out_error_code_or_null = AKU_SUCCESS;
        }
        return false;
    }

    void close() {}
};

struct ConnectionMock : DbConnection
{
    aku_Status write(const aku_Sample &sample) {
        return AKU_SUCCESS;
    }

    std::shared_ptr<DbCursor> search(std::string query) {
        return std::make_shared<CursorMock>();
    }

    int param_id_to_series(aku_ParamId id, char *buffer, size_t buffer_size) {
        std::string strid = std::to_string(id);
        if (strid.size() < buffer_size) {
            memcpy(buffer, strid.data(), strid.size());
            return strid.size();
        }
        return -1*strid.size();
    }

    aku_Status series_to_param_id(const char *name, size_t size, aku_Sample *sample) {
        throw "not implemented";
    }
};

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_query_cursor) {

    std::string expected = "+33\r\n+20141210T074243.111999000\r\n+3.1415\r\n+44\r\n+20141210T122434.999111000\r\n$10\r\nteststring\r\n";
    std::shared_ptr<DbConnection> con;
    con.reset(new ConnectionMock());
    char buffer[0x1000];
    QueryCursor cursor(con, 1000);
    cursor.start();
    size_t len = cursor.read_some(buffer, 0x1000);
    BOOST_REQUIRE(len > 0);
    auto actual = std::string(buffer, buffer + len);
    BOOST_REQUIRE_EQUAL(expected, actual);
}
