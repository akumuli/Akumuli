#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "seriesparser.h"
#include "queryprocessor.h"
#include "datetime.h"

using namespace Akumuli;
using namespace Akumuli::QP;

void logger(aku_LogLevel errlvl, const char* msg) {
    if (errlvl == AKU_LOG_ERROR) {
        std::cout << msg << std::endl;
    }
}

struct NodeMock : Node {
    std::vector<aku_Timestamp> timestamps;
    std::vector<aku_ParamId>   ids;
    std::vector<double>        values;

    // Bolt interface
public:

    NodeType get_type() const { return Node::Mock; }
    void complete() {}
    void set_error(aku_Status status) {
        BOOST_FAIL("set_error shouldn't be called");
    }
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
    s.payload.type = AKU_PAYLOAD_FLOAT;
    s.payload.value.float64 = value;
    return s;
}

BOOST_AUTO_TEST_CASE(Test_stringpool_0) {

    StringPool pool;
    const char* foo = "foo";
    auto result_foo = pool.add(foo, foo + 3, 0ul);
    const char* bar = "123456";
    auto result_bar = pool.add(bar, bar + 6, 0ul);
    BOOST_REQUIRE_EQUAL(result_foo.second, 3);
    BOOST_REQUIRE_EQUAL(std::string(result_foo.first, result_foo.first + result_foo.second), foo);
    BOOST_REQUIRE_EQUAL(result_bar.second, 6);
    BOOST_REQUIRE_EQUAL(std::string(result_bar.first, result_bar.first + result_bar.second), bar);
}

BOOST_AUTO_TEST_CASE(Test_seriesmatcher_0) {

    SeriesMatcher matcher(1ul);
    const char* foo = "foobar";
    const char* bar = "barfoobar";
    const char* buz = "buz";
    matcher.add(foo, foo+6);
    matcher.add(bar, bar+9);

    auto foo_id = matcher.match(foo, foo+6);
    BOOST_REQUIRE_EQUAL(foo_id, 1ul);

    auto bar_id = matcher.match(bar, bar+9);
    BOOST_REQUIRE_EQUAL(bar_id, 2ul);

    auto buz_id = matcher.match(buz, buz+3);
    BOOST_REQUIRE_EQUAL(buz_id, 0ul);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_0) {

    const char* series = " cpu  region=europe   host=127.0.0.1 ";
    auto len = strlen(series);
    char out[40];
    const char* pbegin = nullptr;
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pbegin, &pend);

    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    std::string expected = "cpu host=127.0.0.1 region=europe";
    std::string actual = std::string((const char*)out, pend);
    BOOST_REQUIRE_EQUAL(expected, actual);

    std::string keystr = std::string(pbegin, pend);
    BOOST_REQUIRE_EQUAL("host=127.0.0.1 region=europe", keystr);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_1) {

    const char* series = "cpu";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_2) {

    const char* series = "cpu region host=127.0.0.1 ";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_3) {

    const char* series = "cpu region=europe host";
    auto len = strlen(series);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_4) {

    auto len = AKU_LIMITS_MAX_SNAME + 1;
    char series[len];
    char out[len];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_5) {

    auto len = AKU_LIMITS_MAX_SNAME - 1;
    char series[len];
    char out[10];
    const char* pend = nullptr;
    int status = SeriesParser::to_normal_form(series, series + len, out, out + 10, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_ARG);
}

// Test queryprocessor building

BOOST_AUTO_TEST_CASE(Test_queryprocessor_building_1) {

    SeriesMatcher matcher(1ul);
    const char* series[] = {
        "cpu key1=1 key3=1",
        "cpu key2=2 key3=2",
        "cpu key3=3",
        "cpu key3=4",
    };
    for(int i = 0; i < 4; i++) {
        const char* sname = series[i];
        int slen = strlen(sname);
        matcher.add(sname, sname+slen);
    }
    const char* json = R"(
            {
                "sample": [{ "name": "reservoir", "size": 1000 }],
                "metric": ["cpu", "mem"],
                "range" : {
                    "from": "20150101T000000",
                    "to"  : "20150102T000000"
                },
                "where": [
                    {"in":
                        {"key3": [1, 2, 3] }
                    }
                ]
            }
    )";
    auto terminal = std::make_shared<NodeMock>();
    auto iproc = matcher.build_query_processor(json, terminal, &logger);
    auto qproc = std::dynamic_pointer_cast<QP::ScanQueryProcessor>(iproc);
    BOOST_REQUIRE(qproc->root_node_->get_type() == Node::FilterById);
    BOOST_REQUIRE(qproc->metrics_.size() == 2);
    auto m1 = qproc->metrics_.at(0);
    auto m2 = qproc->metrics_.at(1);
    if (m1 == "cpu") {
        BOOST_REQUIRE(m2 == "mem");
    } else {
        BOOST_REQUIRE(m1 == "mem");
        BOOST_REQUIRE(m2 == "cpu");
    }
    auto first_ts  = boost::posix_time::ptime(boost::gregorian::date(2015, 01, 01));
    auto second_ts = boost::posix_time::ptime(boost::gregorian::date(2015, 01, 02));
    BOOST_REQUIRE(qproc->lowerbound() == DateTimeUtil::from_boost_ptime(first_ts));
    BOOST_REQUIRE(qproc->upperbound() == DateTimeUtil::from_boost_ptime(second_ts));

    qproc->start();
    qproc->put(make(DateTimeUtil::from_boost_ptime(first_ts), 1, 0.123));  // should match
    qproc->put(make(DateTimeUtil::from_boost_ptime(first_ts), 2, 0.234));  // should match
    qproc->put(make(DateTimeUtil::from_boost_ptime(first_ts), 4, 0.345));  // shouldn't match
    qproc->stop();

    BOOST_REQUIRE_EQUAL(terminal->ids.size(), 2);
    BOOST_REQUIRE_EQUAL(terminal->ids.at(0), 1);
    BOOST_REQUIRE_EQUAL(terminal->values.at(0), 0.123);
    BOOST_REQUIRE_EQUAL(terminal->ids.at(1), 2);
    BOOST_REQUIRE_EQUAL(terminal->values.at(1), 0.234);
}
