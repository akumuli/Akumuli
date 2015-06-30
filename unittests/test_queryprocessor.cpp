#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "queryprocessor.h"

using namespace Akumuli;
using namespace Akumuli::QP;

void logger_stub(int level, const char* msg) {
    if (level == AKU_LOG_ERROR) {
        BOOST_MESSAGE(msg);
    }
}

struct NodeMock : Node {
    std::vector<aku_Timestamp> timestamps;
    std::vector<aku_ParamId> ids;
    std::vector<double> values;

    // Bolt interface
public:

    NodeType get_type() const { return Node::Mock; }
    void complete() {}
    bool put(aku_Sample const& s) {
        ids.push_back(s.paramid);
        timestamps.push_back(s.timestamp);
        values.push_back(s.payload.value.float64);
        return true;
    }
};

aku_Sample make(aku_Timestamp t, aku_ParamId id, double value) {
    aku_Sample s;
    s.paramid = id;
    s.timestamp = t;
    s.payload.type = aku_PData::FLOAT;
    s.payload.value.float64 = value;
    return s;
}

BOOST_AUTO_TEST_CASE(Test_random_sampler_0) {

    auto mock = std::make_shared<NodeMock>();
    auto sampler = NodeBuilder::make_random_sampler("reservoir", 5, mock, &logger_stub);


    sampler->put(make(1ul, 1ul, 1.0));
    sampler->put(make(0ul, 0ul, 0.0));
    sampler->put(make(2ul, 2ul, 2.0));
    sampler->put(make(4ul, 4ul, 4.0));
    sampler->put(make(3ul, 3ul, 3.0));
    sampler->complete();

    BOOST_REQUIRE(mock->timestamps[0] == 0ul);
    BOOST_REQUIRE(mock->ids[0] == 0ul);
    BOOST_REQUIRE(mock->timestamps[1] == 1ul);
    BOOST_REQUIRE(mock->ids[1] == 1ul);
    BOOST_REQUIRE(mock->timestamps[2] == 2ul);
    BOOST_REQUIRE(mock->ids[2] == 2ul);
    BOOST_REQUIRE(mock->timestamps[3] == 3ul);
    BOOST_REQUIRE(mock->ids[3] == 3ul);
    BOOST_REQUIRE(mock->timestamps[4] == 4ul);
    BOOST_REQUIRE(mock->ids[4] == 4ul);
}

BOOST_AUTO_TEST_CASE(Test_random_sampler_1) {

    auto mock = std::make_shared<NodeMock>();
    auto sampler = NodeBuilder::make_random_sampler("reservoir", 10, mock, &logger_stub);

    for (uint64_t u = 0; u < 100; u++) {
        sampler->put(make(100ul - u, 1000ul - u, 1.0));
    }
    sampler->complete();

    BOOST_REQUIRE(mock->timestamps.size() == 10);
    for (int i = 1; i < 10; i++) {
        BOOST_REQUIRE(mock->timestamps[i-1] < mock->timestamps[i]);
        BOOST_REQUIRE(mock->ids[i-1] < mock->ids[i]);
    }
}

BOOST_AUTO_TEST_CASE(Test_random_sampler_2) {

    auto mock = std::make_shared<NodeMock>();
    auto sampler = NodeBuilder::make_random_sampler("reservoir", 1000, mock, &logger_stub);

    for (uint64_t u = 0; u < 100; u++) {
        sampler->put(make(100ul - u, 1000ul - u, 1.0));
    }
    sampler->complete();

    BOOST_REQUIRE_EQUAL(mock->timestamps.size(), mock->ids.size());
    BOOST_REQUIRE(mock->timestamps.size() == 100);
    for (int i = 1; i < 100; i++) {
        BOOST_REQUIRE(mock->timestamps[i-1] < mock->timestamps[i]);
        BOOST_REQUIRE(mock->ids[i-1] < mock->ids[i]);
    }
}
