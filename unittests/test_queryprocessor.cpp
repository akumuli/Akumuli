#include <iostream>
#include <memory>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "queryprocessor.h"
#include "query_processing/randomsamplingnode.h"
#include "query_processing/paa.h"
#include "datetime.h"

using namespace Akumuli;
using namespace Akumuli::QP;

void logger_stub(aku_LogLevel level, const char* msg) {
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

    void complete() {}
    void set_error(aku_Status status) {
        BOOST_FAIL("set_error shouldn't be called");
    }
    bool put(aku_Sample const& s) {
        if (s.payload.type < aku_PData::MARGIN) {
            ids.push_back(s.paramid);
            timestamps.push_back(s.timestamp);
            values.push_back(s.payload.float64);
        }
        return true;
    }
    int get_requirements() const { return EMPTY; }
};

aku_Sample make(aku_Timestamp t, aku_ParamId id, double value) {
    aku_Sample s;
    s.paramid = id;
    s.timestamp = t;
    s.payload.type = AKU_PAYLOAD_FLOAT;
    s.payload.float64 = value;
    return s;
}

BOOST_AUTO_TEST_CASE(Test_random_sampler_0) {

    auto mock = std::make_shared<NodeMock>();
    auto sampler = std::make_shared<RandomSamplingNode>(5, mock);

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
    auto sampler = std::make_shared<RandomSamplingNode>(10, mock);

    for (u64 u = 0; u < 100; u++) {
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
    auto sampler = std::make_shared<RandomSamplingNode>(100, mock);

    for (u64 u = 0; u < 100; u++) {
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

BOOST_AUTO_TEST_CASE(Test_moving_average_fwd) {
    aku_Sample margin = {};
    margin.payload.type = aku_PData::HI_MARGIN;
    margin.payload.size = sizeof(aku_Sample);
    auto mock = std::make_shared<NodeMock>();
    auto ma = std::make_shared<MeanPAA>(mock);

    // two parameters
    std::vector<double> p1, p2;
    const int END = 1000;  // 100 steps
    for (int i = 0; i < END; i++) {
        p1.push_back(1.0);
        p2.push_back(2.0);
    }
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    for (int i = 0; i < END; i++) {
        sample.paramid = 0;
        sample.timestamp = i;
        sample.payload.float64 = p1.at(i);
        BOOST_REQUIRE(ma->put(sample));
        sample.paramid = 1;
        sample.timestamp = i;
        sample.payload.float64 = p2.at(i);
        BOOST_REQUIRE(ma->put(sample));
        if (i % 10 == 0) {
            margin.timestamp = i;
            ma->put(margin);
        }
    }
    ma->complete();
    const size_t EXPECTED_SIZE = 200;
    BOOST_REQUIRE_EQUAL(mock->timestamps.size(), EXPECTED_SIZE);
    double values_sum = std::accumulate(mock->values.begin(), mock->values.end(), 0.0,
                                        [](double a, double b) { return a + b; });
    BOOST_REQUIRE_CLOSE(values_sum, 300.0, 0.00001);
    aku_Timestamp ts_sum = std::accumulate(mock->timestamps.begin(), mock->timestamps.end(), 0,
                                           [](aku_Timestamp a, aku_Timestamp b) { return a + b; });
    BOOST_REQUIRE_EQUAL(ts_sum, 99000);
}

BOOST_AUTO_TEST_CASE(Test_moving_average_bwd) {
    aku_Sample margin = {};
    margin.payload.type = aku_PData::LO_MARGIN;
    margin.payload.size = sizeof(aku_Sample);
    auto mock = std::make_shared<NodeMock>();
    auto ma = std::make_shared<MeanPAA>(mock);

    // two parameters
    std::vector<double> p1, p2;
    const int END = 1000;  // 100 steps
    for (int i = 0; i < END; i++) {
        p1.push_back(1.0);
        p2.push_back(2.0);
    }
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    for (int i = END; i --> 0;) {
        sample.paramid = 0;
        sample.timestamp = i;
        sample.payload.float64 = p1.at(i);
        BOOST_REQUIRE(ma->put(sample));
        sample.paramid = 1;
        sample.timestamp = i;
        sample.payload.float64 = p2.at(i);
        BOOST_REQUIRE(ma->put(sample));
        if (i % 10 == 0) {
            margin.timestamp = i;
            ma->put(margin);
        }
    }
    ma->complete();
    const size_t EXPECTED_SIZE = 200;
    BOOST_REQUIRE_EQUAL(mock->timestamps.size(), EXPECTED_SIZE);
    double values_sum = std::accumulate(mock->values.begin(), mock->values.end(), 0.0,
                                        [](double a, double b) { return a + b; });
    BOOST_REQUIRE_CLOSE(values_sum, 300.0, 0.00001);
    aku_Timestamp ts_sum = std::accumulate(mock->timestamps.begin(), mock->timestamps.end(), 0,
                                           [](aku_Timestamp a, aku_Timestamp b) { return a + b; });
    BOOST_REQUIRE_EQUAL(ts_sum, 99000);
}

BOOST_AUTO_TEST_CASE(Test_queryprocessor_building_1) {

    SeriesMatcher matcher(1ul);
    const char* series1[] = {
        "cpu key1=1 key3=1",
        "cpu key2=2 key3=2",
        "cpu key3=3",
        "cpu key3=4",
    };
    for(int i = 0; i < 4; i++) {
        const char* sname = series1[i];
        int slen = strlen(sname);
        matcher.add(sname, sname+slen);
    }
    const char* json = R"(
            {
                "metric": "cpu",
                "range" : {
                    "from": "20150101T000000",
                    "to"  : "20150102T000000"
                },
                "where": {
                    "key3": [1, 2, 3]
                }
            }
    )";
    auto terminal = std::make_shared<NodeMock>();
    auto iproc = QP::Builder::build_query_processor(json, terminal, matcher, &logger_stub);
    auto qproc = std::dynamic_pointer_cast<QP::ScanQueryProcessor>(iproc);
    BOOST_REQUIRE(qproc->metric_ == "cpu");
    auto first_ts  = boost::posix_time::ptime(boost::gregorian::date(2015, 01, 01));
    auto second_ts = boost::posix_time::ptime(boost::gregorian::date(2015, 01, 02));
    BOOST_REQUIRE(qproc->range().lowerbound == DateTimeUtil::from_boost_ptime(first_ts));
    BOOST_REQUIRE(qproc->range().upperbound == DateTimeUtil::from_boost_ptime(second_ts));

    qproc->start();
    if (qproc->filter().apply(1)) {
        qproc->put(make(DateTimeUtil::from_boost_ptime(first_ts), 1, 0.123));  // should match
    } else {
        BOOST_FAIL("bad filter");
    }
    if (qproc->filter().apply(2)) {
        qproc->put(make(DateTimeUtil::from_boost_ptime(first_ts), 2, 0.234));  // should match
    } else {
        BOOST_FAIL("bad filter");
    }
    if (qproc->filter().apply(4)) {
        BOOST_FAIL("bad filter");
    }
    qproc->stop();

    BOOST_REQUIRE_EQUAL(terminal->ids.size(), 2);
    BOOST_REQUIRE_EQUAL(terminal->ids.at(0), 1);
    BOOST_REQUIRE_EQUAL(terminal->values.at(0), 0.123);
    BOOST_REQUIRE_EQUAL(terminal->ids.at(1), 2);
    BOOST_REQUIRE_EQUAL(terminal->values.at(1), 0.234);
}
