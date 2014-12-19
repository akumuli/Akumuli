#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "protocolparser.h"

using namespace Akumuli;

struct Consumer : ProtocolConsumer {

};

BOOST_AUTO_TEST_CASE(Test_protocol_parse_1) {
}
