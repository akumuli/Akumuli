#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

#include "ingestion_pipeline.h"
#include "protocolparser.h"
#include "resp.h"

using namespace Akumuli;

struct ConsumerMock : DbSession {
    std::vector<aku_ParamId>     param_;
    std::vector<aku_Timestamp>   ts_;
    std::vector<double>          data_;

    virtual ~ConsumerMock() {}

    virtual aku_Status write(const aku_Sample &sample) override {
        param_.push_back(sample.paramid);
        ts_.push_back(sample.timestamp);
        data_.push_back(sample.payload.float64);
        return AKU_SUCCESS;
    }

    virtual std::shared_ptr<DbCursor> search(std::string) override {
        throw "Not implemented";
    }

    virtual int param_id_to_series(aku_ParamId id, char* buf, size_t sz) override {
        auto str = std::to_string(id);
        assert(str.size() <= sz);
        memcpy(buf, str.data(), str.size());
        return static_cast<int>(str.size());
    }

    virtual aku_Status series_to_param_id(const char* begin, size_t sz, aku_Sample* sample) override {
        std::string num(begin, begin + sz);
        sample->paramid = boost::lexical_cast<u64>(num);
        return AKU_SUCCESS;
    }
};

void null_deleter(const char*) {}

std::shared_ptr<const char> buffer_from_static_string(const char* str) {
    return std::shared_ptr<const char>(str, &null_deleter);
}


BOOST_AUTO_TEST_CASE(Test_protocol_parse_1) {
    const char *messages = "+1\r\n:2\r\n+34.5\r\n+6\r\n:7\r\n+8.9\r\n";
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock());
    ProtocolParser parser(cons);
    auto buf = parser.get_next_buffer();
    memcpy(buf, messages, 29);
    parser.start();
    parser.parse_next(buf, 29);
    parser.close();

    BOOST_REQUIRE_EQUAL(cons->param_[0], 1);
    BOOST_REQUIRE_EQUAL(cons->param_[1], 6);
    BOOST_REQUIRE_EQUAL(cons->ts_[0], 2);
    BOOST_REQUIRE_EQUAL(cons->ts_[1], 7);
    BOOST_REQUIRE_EQUAL(cons->data_[0], 34.5);
    BOOST_REQUIRE_EQUAL(cons->data_[1], 8.9);
}

BOOST_AUTO_TEST_CASE(Test_protocol_parse_2) {

    const char *message1 = "+1\r\n:2\r\n+34.5\r\n+6\r\n:7\r\n+8.9";
    const char *message2 = "\r\n+10\r\n:11\r\n+12.13\r\n+14\r\n:15\r\n+16.7\r\n";

    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();

    auto buf = parser.get_next_buffer();
    memcpy(buf, message1, 27);
    parser.parse_next(buf, 27);

    BOOST_REQUIRE_EQUAL(cons->param_.size(), 1);
    // 0
    BOOST_REQUIRE_EQUAL(cons->param_[0], 1);
    BOOST_REQUIRE_EQUAL(cons->ts_[0], 2);
    BOOST_REQUIRE_EQUAL(cons->data_[0], 34.5);

    buf = parser.get_next_buffer();
    memcpy(buf, message2, 37);
    parser.parse_next(buf, 37);

    BOOST_REQUIRE_EQUAL(cons->param_.size(), 4);
    // 1
    BOOST_REQUIRE_EQUAL(cons->param_[1], 6);
    BOOST_REQUIRE_EQUAL(cons->ts_[1], 7);
    BOOST_REQUIRE_EQUAL(cons->data_[1], 8.9);
    // 2
    BOOST_REQUIRE_EQUAL(cons->param_[2], 10);
    BOOST_REQUIRE_EQUAL(cons->ts_[2], 11);
    BOOST_REQUIRE_EQUAL(cons->data_[2], 12.13);
    // 3
    BOOST_REQUIRE_EQUAL(cons->param_[3], 14);
    BOOST_REQUIRE_EQUAL(cons->ts_[3], 15);
    BOOST_REQUIRE_EQUAL(cons->data_[3], 16.7);
    parser.close();
}

BOOST_AUTO_TEST_CASE(Test_protocol_parse_error_format) {
    const char *messages = "+1\r\n:2\r\n+34.5\r\n+2\r\n:d\r\n+8.9\r\n";
    auto check_resp_error = [](const RESPError& error) {
        auto bl = error.get_bottom_line();
        std::string what = error.what();
        auto bls = bl.size();
        if (bls == 0) {
            return false;
        }
        if (bls == what.size()) {
            return false;
        }
        auto c = what[bls-1];
        return c == 'd';
    };
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();
    auto buf = parser.get_next_buffer();
    memcpy(buf, messages, 29);
    BOOST_REQUIRE_EXCEPTION(parser.parse_next(buf, 29), RESPError, check_resp_error);
}

void find_framing_issues(const char* message, size_t msglen, size_t pivot1, size_t pivot2) {

    auto buffer1 = buffer_from_static_string(message);
    auto buffer2 = buffer_from_static_string(message + pivot1);
    auto buffer3 = buffer_from_static_string(message + pivot2);

    PDU pdu1 = {
        buffer1,
        static_cast<u32>(pivot1),
        0u
    };
    PDU pdu2 = {
        buffer2,
        static_cast<u32>(pivot2 - pivot1),
        0u
    };
    PDU pdu3 = {
        buffer3,
        static_cast<u32>(msglen - pivot2),
        0u
    };

    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();
    auto buf = parser.get_next_buffer();
    memcpy(buf, message, pivot1);
    parser.parse_next(buf, pivot1);

    buf = parser.get_next_buffer();
    memcpy(buf, message + pivot1, pivot2 - pivot1);
    parser.parse_next(buf, pivot2 - pivot1);

    buf = parser.get_next_buffer();
    memcpy(buf, message + pivot2, msglen - pivot2);
    parser.parse_next(buf, msglen - pivot2);

    parser.close();

    BOOST_REQUIRE_EQUAL(cons->param_.size(), 4);
    // 0
    BOOST_REQUIRE_EQUAL(cons->param_[0], 1);
    BOOST_REQUIRE_EQUAL(cons->ts_[0], 2);
    BOOST_REQUIRE_EQUAL(cons->data_[0], 34.5);
    // 1
    BOOST_REQUIRE_EQUAL(cons->param_[1], 6);
    BOOST_REQUIRE_EQUAL(cons->ts_[1], 7);
    BOOST_REQUIRE_EQUAL(cons->data_[1], 8.9);
    // 2
    BOOST_REQUIRE_EQUAL(cons->param_[2], 10);
    BOOST_REQUIRE_EQUAL(cons->ts_[2], 11);
    BOOST_REQUIRE_EQUAL(cons->data_[2], 12.13);
    // 3
    BOOST_REQUIRE_EQUAL(cons->param_[3], 14);
    BOOST_REQUIRE_EQUAL(cons->ts_[3], 15);
    BOOST_REQUIRE_EQUAL(cons->data_[3], 16.7);

}

/**
 * This test is created to find nontrivial framing issues in protocol parser.
 * Everything works fine when PDU contains entire record (series, timestamp and value)
 * but in the real world scenario this envariant can be broken and each record can be
 * scattered between many PDUs.
 */
BOOST_AUTO_TEST_CASE(Test_protocol_parser_framing) {

    const char *message = "+1\r\n:2\r\n+34.5\r\n"
                          "+6\r\n:7\r\n+8.9\r\n"
                          "+10\r\n:11\r\n+12.13\r\n"
                          "+14\r\n:15\r\n+16.7\r\n";

    size_t msglen = strlen(message);

    for (int i = 0; i < 100; i++) {
        size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
        size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
        find_framing_issues(message, msglen, pivot1, pivot2);
    }
}
