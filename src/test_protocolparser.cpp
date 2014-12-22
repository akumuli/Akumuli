#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "protocolparser.h"

using namespace Akumuli;

struct ConsumerMock : ProtocolConsumer {
    std::vector<aku_ParamId>     param_;
    std::vector<aku_TimeStamp>   ts_;
    std::vector<double>          data_;

    void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        param_.push_back(param);
        ts_.push_back(ts);
        data_.push_back(data);
    }
};

void null_deleter(const char* s) {}

std::shared_ptr<const Byte> buffer_from_static_string(const char* str) {
    return std::shared_ptr<const Byte>(str, &null_deleter);
}

BOOST_AUTO_TEST_CASE(Test_protocol_parse_1) {

    const char *messages = ":1\r\n:2\r\n+34.5\r\n:6\r\n:7\r\n+8.9\r\n";
    auto buffer = buffer_from_static_string(messages);
    PDU pdu = {
        buffer,
        29,
        0u
    };
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();
    parser.parse_next(pdu);
    parser.close();
    BOOST_REQUIRE_EQUAL(cons->param_[0], 1);
    BOOST_REQUIRE_EQUAL(cons->param_[1], 6);
    BOOST_REQUIRE_EQUAL(cons->ts_[0], 2);
    BOOST_REQUIRE_EQUAL(cons->ts_[1], 7);
    BOOST_REQUIRE_EQUAL(cons->data_[0], 34.5);
    BOOST_REQUIRE_EQUAL(cons->data_[1], 8.9);
}
