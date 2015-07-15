#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "protocolparser.h"
#include "resp.h"

using namespace Akumuli;

struct ConsumerMock : ProtocolConsumer {
    std::vector<aku_ParamId>     param_;
    std::vector<aku_Timestamp>   ts_;
    std::vector<double>          data_;
    std::vector<std::string>     bulk_;

    void write(const aku_Sample& sample) {
        param_.push_back(sample.paramid);
        ts_.push_back(sample.timestamp);
        data_.push_back(sample.payload.value.float64);
    }

    void add_bulk_string(const Byte *buffer, size_t n) {
        bulk_.push_back(std::string(buffer, buffer + n));
    }

    aku_Status series_to_param_id(const char *str, size_t strlen, aku_Sample *sample) {
        throw "not implemented";
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
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock());
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

BOOST_AUTO_TEST_CASE(Test_protocol_parse_2) {

    const char *message1 = ":1\r\n:2\r\n+34.5\r\n:6\r\n:7\r\n+8.9";
    const char *message2 = "\r\n:10\r\n:11\r\n+12.13\r\n:14\r\n:15\r\n+16.7\r\n";
    auto buffer1 = buffer_from_static_string(message1);
    auto buffer2 = buffer_from_static_string(message2);
    PDU pdu1 = {
        buffer1,
        27,
        0u
    };
    PDU pdu2 = {
        buffer2,
        37,
        0u
    };
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();
    parser.parse_next(pdu1);

    BOOST_REQUIRE_EQUAL(cons->param_.size(), 1);
    // 0
    BOOST_REQUIRE_EQUAL(cons->param_[0], 1);
    BOOST_REQUIRE_EQUAL(cons->ts_[0], 2);
    BOOST_REQUIRE_EQUAL(cons->data_[0], 34.5);
    parser.parse_next(pdu2);

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

BOOST_AUTO_TEST_CASE(Test_protocol_parse_bulk_strings) {

    const char *message1 = "$12\r\n123456";
    const char *message2 = "789ABC\r\n";
    auto buffer1 = buffer_from_static_string(message1);
    auto buffer2 = buffer_from_static_string(message2);
    PDU pdu1 = {
        buffer1,
        11,
        0u
    };
    PDU pdu2 = {
        buffer2,
        8,
        0u
    };
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    ProtocolParser parser(cons);
    parser.start();
    parser.parse_next(pdu1);

    BOOST_REQUIRE_EQUAL(cons->bulk_.size(), 0);
    parser.parse_next(pdu2);
    BOOST_REQUIRE_EQUAL(cons->bulk_.size(), 1);
    // 1
    BOOST_REQUIRE_EQUAL(cons->bulk_[0], "123456789ABC");
    parser.close();
}


BOOST_AUTO_TEST_CASE(Test_protocol_parse_error_format) {

    const char *messages = ":1\r\n:2\r\n+34.5\r\n:d\r\n:7\r\n+8.9\r\n";
    auto buffer = buffer_from_static_string(messages);
    PDU pdu = {
        buffer,
        29,
        0u
    };
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
    BOOST_REQUIRE_EXCEPTION(parser.parse_next(pdu), RESPError, check_resp_error);
}
