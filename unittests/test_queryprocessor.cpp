#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "queryprocessor.h"

using namespace Akumuli;

void logger_stub(int level, const char* msg) {
    if (level == AKU_LOG_ERROR) {
        BOOST_MESSAGE(msg);
    }
}

struct BoltMock : Bolt {
    std::vector<aku_Timestamp> timestamps;
    std::vector<aku_ParamId>   ids;
    std::vector<double>        values;

    // Bolt interface
public:

    BoltType get_bolt_type() const { return Bolt::Mock; }
    void complete(std::shared_ptr<Bolt> caller) {}
    void add_output(std::shared_ptr<Bolt> next) {}
    void add_input(std::weak_ptr<Bolt> input)   {}
    virtual std::vector<std::shared_ptr<Bolt> > get_bolt_inputs() const { return {}; }
    virtual std::vector<std::shared_ptr<Bolt> > get_bolt_outputs() const { return {}; }

    void put(aku_Timestamp ts, aku_ParamId id, double value) {
        timestamps.push_back(ts);
        ids.push_back(id);
        values.push_back(value);
    }
};

BOOST_AUTO_TEST_CASE(Test_random_sampler_0) {

    auto sampler = BoltsBuilder::make_random_sampler("reservoir", 5, &logger_stub);
    auto mock = std::make_shared<BoltMock>();
    sampler->add_output(mock);
    sampler->put(1ul, 1ul, 1.0);
    sampler->put(0ul, 0ul, 0.0);
    sampler->put(2ul, 2ul, 2.0);
    sampler->put(4ul, 4ul, 4.0);
    sampler->put(3ul, 3ul, 3.0);
    sampler->complete(std::shared_ptr<Bolt>());

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

    auto sampler = BoltsBuilder::make_random_sampler("reservoir", 10, &logger_stub);
    auto mock = std::make_shared<BoltMock>();
    sampler->add_output(mock);
    for (uint64_t u = 0; u < 100; u++) {
        sampler->put(100ul - u, 1000ul - u, 1.0);
    }
    sampler->complete(std::shared_ptr<Bolt>());

    BOOST_REQUIRE(mock->timestamps.size() == 10);
    for (int i = 1; i < 10; i++) {
        BOOST_REQUIRE(mock->timestamps[i-1] < mock->timestamps[i]);
        BOOST_REQUIRE(mock->ids[i-1] < mock->ids[i]);
    }
}

BOOST_AUTO_TEST_CASE(Test_random_sampler_2) {

    auto sampler = BoltsBuilder::make_random_sampler("reservoir", 1000, &logger_stub);
    auto mock = std::make_shared<BoltMock>();
    sampler->add_output(mock);
    for (uint64_t u = 0; u < 100; u++) {
        sampler->put(100ul - u, 1000ul - u, 1.0);
    }
    sampler->complete(std::shared_ptr<Bolt>());

    BOOST_REQUIRE_EQUAL(mock->timestamps.size(), mock->ids.size());
    BOOST_REQUIRE(mock->timestamps.size() == 100);
    for (int i = 1; i < 100; i++) {
        BOOST_REQUIRE(mock->timestamps[i-1] < mock->timestamps[i]);
        BOOST_REQUIRE(mock->ids[i-1] < mock->ids[i]);
    }
}
