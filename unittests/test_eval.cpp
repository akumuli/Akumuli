#include <iostream>
#include <random>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <algorithm>

#include <boost/property_tree/json_parser.hpp>
#include "query_processing/eval.h"
#include "queryprocessor_framework.h"

using namespace Akumuli::QP;
using namespace Akumuli;

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

struct BigSample : aku_Sample {
    char pad[1024];
};

void init_request(ReshapeRequest *req) {
    // Build series names
    std::vector<std::string> names = {
        "col0 foo=bar",
        "col1 foo=bar",
        "col2 foo=bar",
        "col3 foo=bar",
        "col4 foo=bar",
        "col5 foo=bar",
        "col6 foo=bar",
        "col7 foo=bar",
        "col8 foo=bar",
        "col9 foo=bar",
    };
    std::vector<aku_ParamId> ids = {
        1000,
        1001,
        1002,
        1003,
        1004,
        1005,
        1006,
        1007,
        1008,
        1009,
    };
    req->select.columns.resize(ids.size());
    req->select.matcher.reset(new PlainSeriesMatcher());
    for (u32 i = 0; i < ids.size(); i++) {
        req->select.columns.at(i).ids.push_back(ids[i]);
        req->select.matcher->_add(names[i], static_cast<i64>(ids[i]));
    }
    req->select.global_matcher = req->select.matcher.get();
}

void init_sample(aku_Sample& src, std::initializer_list<double>&& list) {
    src = {};
    src.paramid = 42;
    src.timestamp = 112233;
    if (list.size() == 1) {
        src.payload.type = AKU_PAYLOAD_FLOAT;
        src.payload.size = sizeof(aku_Sample);
        src.payload.float64 = *list.begin();
    }
    else {
        char* dest = src.payload.data;
        int cnt = 0;
        for (auto it = list.begin(); it != list.end(); it++) {
            double value = *it;
            memcpy(dest, &value, sizeof(value));
            dest += sizeof(value);
            cnt++;
        }
        u64 mask = (1 << cnt) - 1;
        memcpy(&src.payload.float64, &mask, sizeof(mask));
        src.payload.size = sizeof(aku_Sample) + sizeof(double)*cnt;
        src.payload.type = AKU_PAYLOAD_TUPLE;
    }
}

boost::property_tree::ptree init_ptree(const char* tc) {
    std::stringstream json;
    json << tc;
    boost::property_tree::ptree ptree;
    boost::property_tree::json_parser::read_json(json, ptree);
    return ptree;
}

BOOST_AUTO_TEST_CASE(Test_eval_1) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["+", 1, 2, 3, 4])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    aku_Sample src;
    init_sample(src, {11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
}

BOOST_AUTO_TEST_CASE(Test_eval_2) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["+", "col0", 2, 3, 4])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    aku_Sample src = {};
    init_sample(src, {11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 20);
}

BOOST_AUTO_TEST_CASE(Test_eval_3) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["+", "col0", 2, 3, 4, ["*", 3, 3]])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    aku_Sample src;
    init_sample(src, {11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 29);
}

BOOST_AUTO_TEST_CASE(Test_eval_4) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["*", "col0", "col1", "col3"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {3, 5, 7, 11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 165);
}

BOOST_AUTO_TEST_CASE(Test_eval_5) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["+", "col2", 28, ["*", "col0", "col1", "col3"]])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {3, 5, 7, 11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 200);
}

BOOST_AUTO_TEST_CASE(Test_eval_6) {
    // Test min function
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["min", "col1", 10, "col0", "col2", "col3"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {3, 5, 7, 11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 3);
}

BOOST_AUTO_TEST_CASE(Test_eval_6_fold) {
    // Test min function with const propogation
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["min", "1", 10, "-10", "2", "100"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, -10);
}

BOOST_AUTO_TEST_CASE(Test_eval_7) {
    // Test max function
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["max", "col1", 10, "col0", "col2", "col3"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {3, 5, 7, 11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 11);
}

BOOST_AUTO_TEST_CASE(Test_eval_7_fold) {
    // Test max function with const propogation
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["max", "1", 10, "-10", "2", "100"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 100);
}

BOOST_AUTO_TEST_CASE(Test_eval_8_fold) {
    // Test nested func-call folding
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["max", "1", 10, ["min", "-10", "2", "100"]])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
}

BOOST_AUTO_TEST_CASE(Test_eval_9_sma) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["sma", 2, "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {10});
    MutableSample ms0(&src);
    eval.put(ms0);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {20});
    MutableSample ms1(&src);
    eval.put(ms1);
    BOOST_REQUIRE_EQUAL(next->result_, 15);
    init_sample(src, {30});
    MutableSample ms2(&src);
    eval.put(ms2);
    BOOST_REQUIRE_EQUAL(next->result_, 25);
    init_sample(src, {40});
    MutableSample ms3(&src);
    eval.put(ms3);
    BOOST_REQUIRE_EQUAL(next->result_, 35);
    init_sample(src, {50});
    MutableSample ms4(&src);
    eval.put(ms4);
    BOOST_REQUIRE_EQUAL(next->result_, 45);
    init_sample(src, {60});
    MutableSample ms5(&src);
    eval.put(ms5);
    BOOST_REQUIRE_EQUAL(next->result_, 55);
}

BOOST_AUTO_TEST_CASE(Test_eval_10_abs) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["abs", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {-3});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 3);
}

BOOST_AUTO_TEST_CASE(Test_eval_10_abs_fold) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["abs", -10])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {-3});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
}

BOOST_AUTO_TEST_CASE(Test_eval_11_deriv) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["deriv1", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    const aku_Timestamp sec = 1000000000;
    BigSample src;
    init_sample(src, {10});
    src.timestamp = sec;
    MutableSample ms0(&src);
    eval.put(ms0);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {20});
    src.timestamp = sec*2;
    MutableSample ms1(&src);
    eval.put(ms1);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {30});
    src.timestamp = sec*3;
    MutableSample ms2(&src);
    eval.put(ms2);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {40});
    src.timestamp = sec*4;
    MutableSample ms3(&src);
    eval.put(ms3);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {50});
    src.timestamp = sec*5;
    MutableSample ms4(&src);
    eval.put(ms4);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
    init_sample(src, {60});
    src.timestamp = sec*6;
    MutableSample ms5(&src);
    eval.put(ms5);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_sub_1) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", "col0", "col1", 2, 3])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {10, 1});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 4);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_sub_2) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", 24, 6, "col0", "col1", 2, 3])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {10, 1});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 2);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_sub_3) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", 24, 6, "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {10, 1});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 8);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_negate) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {10});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, -10);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_sub_folded) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", 10, 1, 2, 3, 2])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 2);
}

BOOST_AUTO_TEST_CASE(Test_eval_12_negate_folded) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["-", "11"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, -11);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_1) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", "col0", "col1", 2])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {24, 3});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 4);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_2) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 24, "col1", 2])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {24, 3});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 4);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_3) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 24, 3, "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {2});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 4);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_0) {
    // division by zero handling
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 24, 3, "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE(std::isnan(next->result_));
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_inverted) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {2});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_CLOSE_FRACTION(next->result_, 0.5, 0.000001);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_inverted_0) {
    // division by zero handling
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {0});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE(std::isnan(next->result_));
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_folded) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 24, 3, 2])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {2});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 4);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_folded_0) {
    // division by zero handling
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 24, 0, 2])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE(std::isnan(next->result_));
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_inv_folded) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 4])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_CLOSE_FRACTION(next->result_, 0.25, 0.000001);
}

BOOST_AUTO_TEST_CASE(Test_eval_13_div_inv_folded_0) {
    // division by zero handling
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["/", 0])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE(std::isnan(next->result_));
}

BOOST_AUTO_TEST_CASE(Test_eval_14_eq_true) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "col0", "col1", "col2"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42, 42, 42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_eq_false) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "col0", "col1", "col2"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42, 24, 42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 0);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_const_eq_true) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "42", "42", "42"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_const_eq_false) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "42", "42", "24"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 0);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_part_eq_true) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "42", "42", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_part_eq_false1) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "24", "42", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 0);
}

BOOST_AUTO_TEST_CASE(Test_eval_14_part_eq_false2) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["==", "42", "42", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {24});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 0);
}

BOOST_AUTO_TEST_CASE(Test_eval_15_not_equal) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["!=", "42", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {24});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_16_less) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["<", "24", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_17_less_or_equal) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"(["<=", 24, 42, "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_18_greater) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"([">", "42", "col0"])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {24});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_eval_19_greater_or_equal) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree(R"([">=", "42", "col0", 24])");
    auto next = std::make_shared<MockNode>();
    Eval eval(ptree, req, next, true);
    BigSample src;
    init_sample(src, {42});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 1);
}

BOOST_AUTO_TEST_CASE(Test_expr_eval_1) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree("{\"expr\":\"1 + 2 + 3 + 4\"}");
    auto next = std::make_shared<MockNode>();
    ExprEval eval(ptree, req, next);
    aku_Sample src;
    init_sample(src, {11});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 10);
}

BOOST_AUTO_TEST_CASE(Test_expr_eval_2) {
    ReshapeRequest req;
    init_request(&req);
    auto ptree = init_ptree("{\"expr\":\"1 + 2 + 3 + col0 + col1\"}");
    auto next = std::make_shared<MockNode>();
    ExprEval eval(ptree, req, next);
    BigSample src;
    init_sample(src, {4, 5});
    MutableSample ms(&src);
    eval.put(ms);
    BOOST_REQUIRE_EQUAL(next->result_, 15);
}


