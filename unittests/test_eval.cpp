#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <algorithm>

#include <boost/property_tree/json_parser.hpp>
#include "query_processing/eval.h"

using namespace Akumuli::QP;

struct MockNode : Node {
    aku_Status status_;
    double result_;

    MockNode()
        : status_(AKU_SUCCESS)
        , result_(NAN)
    {}

    void complete() {}
    bool put(MutableSample &sample) {
        result_ = *sample[0];
        return true;
    }
    void set_error(aku_Status status) { status_ = status; }
    int get_requirements() const {
        return 0;
    }
};

BOOST_AUTO_TEST_CASE(Test_eval_1) {
    const char* tc = R"(["+", 1, 2, 3, 4])";
    std::stringstream json;
    json << tc;
    boost::property_tree::ptree ptree;
    boost::property_tree::json_parser::read_json(json, ptree);
    auto next = std::make_shared<MockNode>();
    std::unique_ptr<Eval> eval;
    eval.reset(new Eval(ptree, next, true));
    aku_Sample src = {};
    src.paramid = 42;
    src.timestamp = 112233;
    src.payload.type = AKU_PAYLOAD_FLOAT;
    src.payload.size = sizeof(aku_Sample);
    MutableSample ms(&src);
    eval->put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    eval.reset();
}


