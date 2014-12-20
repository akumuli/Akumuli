#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "protocolparser.h"

using namespace Akumuli;

struct ConsumerMock : ProtocolConsumer {
    aku_ParamId     param_;
    aku_TimeStamp   ts_;
    double          data_;

    void write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
        param_ = param;
        ts_ = ts;
        data_ = data;
    }
};

void null_deleter(const char* s) {}

std::shared_ptr<const Byte> buffer_from_static_string(const char* str) {
    return std::shared_ptr<const Byte>(str, &null_deleter);
}

BOOST_AUTO_TEST_CASE(Test_protocol_parse_1) {

    const Byte *messages = ":1\\r\\n:2\\r\\n+34.5\\r\\n:6\\r\\n:7\\r\\n+8.9\\r\\n";
    auto buffer = buffer_from_static_string(messages);
    PDU pdu = {
        buffer,
        sizeof(messages),
        0u
    };
    std::unique_ptr<ProtocolConsumer> cons(new ConsumerMock);
    ProtocolParser parser(std::move(cons));
    parser.parse_next(pdu);
}
