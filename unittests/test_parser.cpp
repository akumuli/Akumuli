#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "index/seriesparser.h"
#include "queryprocessor_framework.h"
#include "datetime.h"
#include <tuple>

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

    int get_requirements() const { return TERMINAL; }
    void complete() {}
    void set_error(aku_Status status) {
        BOOST_FAIL("set_error shouldn't be called");
    }
    bool put(aku_Sample const& s) {
        ids.push_back(s.paramid);
        timestamps.push_back(s.timestamp);
        values.push_back(s.payload.float64);
        return true;
    }
};

aku_Sample make(aku_Timestamp t, aku_ParamId id, double value) {
    aku_Sample s;
    s.paramid = id;
    s.timestamp = t;
    s.payload.type = AKU_PAYLOAD_FLOAT;
    s.payload.float64 = value;
    return s;
}

BOOST_AUTO_TEST_CASE(Test_stringpool_0) {

    StringPool pool;
    const char* foo = "foo";
    auto id_foo = pool.add(foo, foo + 3);
    auto result_foo = pool.str(id_foo);
    const char* bar = "123456";
    auto id_bar = pool.add(bar, bar + 6);
    auto result_bar = pool.str(id_bar);
    BOOST_REQUIRE_EQUAL(std::string(result_foo.first, result_foo.first + result_foo.second), foo);
    BOOST_REQUIRE_EQUAL(std::string(result_bar.first, result_bar.first + result_bar.second), bar);
}

BOOST_AUTO_TEST_CASE(Test_seriesmatcher_0) {

    SeriesMatcher matcher(1ul);
    const char* foo = "foo ba=r";
    const char* bar = "bar foo=bar";
    const char* buz = "buz b=uz";
    auto exp_foo = matcher.add(foo, foo+8);
    auto exp_bar = matcher.add(bar, bar+11);

    auto foo_id = matcher.match(foo, foo+8);
    BOOST_REQUIRE_EQUAL(foo_id, 1ul);
    BOOST_REQUIRE_EQUAL(foo_id, exp_foo);

    auto bar_id = matcher.match(bar, bar+11);
    BOOST_REQUIRE_EQUAL(bar_id, 2ul);
    BOOST_REQUIRE_EQUAL(bar_id, exp_bar);

    auto buz_id = matcher.match(buz, buz+8);
    BOOST_REQUIRE_EQUAL(buz_id, 0ul);
}

BOOST_AUTO_TEST_CASE(Test_seriesmatcher_1) {

    LegacyStringPool spool;
    const char* foo = "host=1 region=A";
    const char* bar = "host=1 region=B";
    const char* buz = "host=2 region=C";

    // Insert first
    spool.add(foo, foo+strlen(foo));

    StringPoolOffset offset = {};  // zero offset initially
    auto res = spool.regex_match("host=1 \\w+=\\w", &offset);
    BOOST_REQUIRE_EQUAL(res.size(), 1u);
    BOOST_REQUIRE(strcmp(foo, res.at(0).first) == 0);
    BOOST_REQUIRE_EQUAL(res.at(0).second, strlen(foo));

    // Insert next
    spool.add(bar, bar+strlen(bar));

    // Continue search
    res = spool.regex_match("host=1 \\w+=\\w", &offset);
    BOOST_REQUIRE_EQUAL(res.size(), 1u);
    BOOST_REQUIRE(strcmp(bar, res.at(0).first) == 0);
    BOOST_REQUIRE_EQUAL(res.at(0).second, strlen(bar));

    // Insert last
    spool.add(buz, buz+strlen(buz));
    res = spool.regex_match("host=1 \\w+=\\w", &offset);
    BOOST_REQUIRE_EQUAL(res.size(), 0u);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_0) {

    const char* series1 = " cpu  region=europe   host=127.0.0.1 ";
    auto len = strlen(series1);
    char out[40];
    const char* pbegin = nullptr;
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + len, &pbegin, &pend);

    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    std::string expected = "cpu host=127.0.0.1 region=europe";
    std::string actual = std::string(static_cast<const char*>(out), pend);
    BOOST_REQUIRE_EQUAL(expected, actual);

    std::string keystr = std::string(pbegin, pend);
    BOOST_REQUIRE_EQUAL("host=127.0.0.1 region=europe", keystr);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_1) {

    const char* series1 = "cpu";
    auto len = strlen(series1);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_2) {

    const char* series1 = "cpu region host=127.0.0.1 ";
    auto len = strlen(series1);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_3) {

    const char* series1 = "cpu region=europe host";
    auto len = strlen(series1);
    char out[27];
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_4) {

    auto len = AKU_LIMITS_MAX_SNAME + 1;
    char series1[len];
    char out[len];
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + len, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_DATA);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_5) {

    auto len = AKU_LIMITS_MAX_SNAME - 1;
    char series1[len];
    char out[10];
    const char* pend = nullptr;
    int status = SeriesParser::to_canonical_form(series1, series1 + len, out, out + 10, &pend, &pend);
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_ARG);
}

BOOST_AUTO_TEST_CASE(Test_seriesparser_6) {
    const char* tags[] = {
        "tag2",
        "tag4",
        "tag7",  // doesn't exists in series name
    };
    const char* series = "metric tag1=1 tag2=2 tag3=3 tag4=4 tag5=5";
    auto name = std::make_pair(series, strlen(series));
    char out[AKU_LIMITS_MAX_SNAME];
    aku_Status status;
    SeriesParser::StringT result;
    StringTools::SetT filter = StringTools::create_set(2);
    filter.insert(std::make_pair(tags[0], 4));
    filter.insert(std::make_pair(tags[1], 4));
    filter.insert(std::make_pair(tags[2], 4));
    std::tie(status, result) = SeriesParser::filter_tags(name, filter, out);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(std::string("metric tag2=2 tag4=4"), std::string(result.first, result.first + result.second));
}

BOOST_AUTO_TEST_CASE(Test_index_0) {
    SeriesMatcher matcher(10ul);
    std::vector<std::string> names = {
        "foo tagA=1 tagB=1",
        "foo tagA=1 tagB=2",
        "foo tagA=1 tagB=3",
        "foo tagA=1 tagB=4",
        "foo tagA=2 tagB=1",
        "foo tagA=2 tagB=2",
        "foo tagA=2 tagB=3",
        "foo tagA=2 tagB=4",
    };
    std::vector<u64> ids;
    for (auto name: names) {
        auto id = matcher.add(name.data(), name.data() + name.size());
        if (id == 0) {
            BOOST_FAIL("Bad id");
        }
        ids.push_back(id);
    }
    MetricName mname("foo");
    std::vector<TagValuePair> tags = {
        TagValuePair("tagA=1")
    };
    IncludeIfAllTagsMatch query(mname, tags.begin(), tags.end());
    auto res = matcher.search(query);
    BOOST_REQUIRE_EQUAL(res.size(), 4);
    int i = 0;
    u64 exp_id = 10ul;
    for (auto tup: res) {
        const char* name;
        int size;
        u64 id;
        std::tie(name, size, id) = tup;
        BOOST_REQUIRE_EQUAL(std::string(name, name + size), names[i]);
        BOOST_REQUIRE_EQUAL(id, exp_id);
        i++;
        exp_id++;
    }
}

BOOST_AUTO_TEST_CASE(Test_index_1) {
    u64 base_id = 10ul;
    SeriesMatcher matcher(base_id);
    std::vector<std::string> names = {
        "foo tagA=1 tagB=1",
        "foo tagA=1 tagB=2",
        "foo tagA=1 tagB=3",
        "foo tagA=1 tagB=4",
        "foo tagA=2 tagB=1",
        "foo tagA=2 tagB=2",
        "foo tagA=2 tagB=3",
        "foo tagA=2 tagB=4",
    };
    std::vector<u64> ids;
    for (auto name: names) {
        auto id = matcher.add(name.data(), name.data() + name.size());
        if (id == 0) {
            BOOST_FAIL("Bad id");
        }
        ids.push_back(id);
    }
    MetricName mname("foo");
    std::vector<TagValuePair> tags = {
        TagValuePair("tagA=2"),
        TagValuePair("tagB=3")
    };

    IncludeIfAllTagsMatch query(mname, tags.begin(), tags.end());
    auto res = matcher.search(query);
    BOOST_REQUIRE_EQUAL(res.size(), 1);
    int i = 0;
    std::vector<u64> offsets = {
        6,
    };
    for (auto tup: res) {
        const char* name;
        int size;
        u64 id;
        std::tie(name, size, id) = tup;
        std::string exp_name = names[offsets[i]];
        BOOST_REQUIRE_EQUAL(std::string(name, name + size), exp_name);
        BOOST_REQUIRE_EQUAL(id, base_id + offsets[i]);
        i++;
    }
}

BOOST_AUTO_TEST_CASE(Test_index_2) {
    u64 base_id = 10ul;
    SeriesMatcher matcher(base_id);
    std::vector<std::string> names = {
        "foo tagA=1 tagB=1",
        "foo tagA=1 tagB=2",
        "foo tagA=1 tagB=3",
        "foo tagA=1 tagB=4",
        "foo tagA=2 tagB=1",
        "foo tagA=2 tagB=2",
        "foo tagA=2 tagB=3",
        "foo tagA=2 tagB=4",
    };
    std::vector<u64> ids;
    for (auto name: names) {
        auto id = matcher.add(name.data(), name.data() + name.size());
        if (id == 0) {
            BOOST_FAIL("Bad id");
        }
        ids.push_back(id);
    }
    MetricName mname("bar");
    std::vector<TagValuePair> tags = {
        TagValuePair("tagA=1"),
    };

    IncludeIfAllTagsMatch query(mname, tags.begin(), tags.end());
    auto res = matcher.search(query);
    BOOST_REQUIRE_EQUAL(res.size(), 0);
}

BOOST_AUTO_TEST_CASE(Test_index_3) {
    u64 base_id = 10ul;
    SeriesMatcher matcher(base_id);
    std::vector<std::string> names = {
        "foo tagA=1 tagB=1 tagC=2",
        "foo tagA=1 tagB=2 tagD=1",
        "foo tagA=1 tagB=3 tagC=8",
        "foo tagA=1 tagB=4 tagC=2",
        "foo tagA=2 tagB=1 tagC=3",
        "foo tagA=2 tagB=2 tagD=0",
        "foo tagA=2 tagB=3 tagC=9",
        "foo tagA=2 tagB=4 tagC=4",
    };
    std::vector<u64> ids;
    for (auto name: names) {
        auto id = matcher.add(name.data(), name.data() + name.size());
        if (id == 0) {
            BOOST_FAIL("Bad id");
        }
        ids.push_back(id);
    }
    std::vector<TagValuePair> tags = {
        TagValuePair("tagD=2"),
    };
    std::vector<std::string> qtags = {"tagD"};
    IncludeIfHasTag query("foo", qtags);
    auto res = matcher.search(query);
    BOOST_REQUIRE_EQUAL(res.size(), 2);
    int i = 0;
    std::vector<u64> offsets = {
        1,
        5,
    };
    for (auto tup: res) {
        const char* name;
        int size;
        u64 id;
        std::tie(name, size, id) = tup;
        std::string exp_name = names[offsets[i]];
        BOOST_REQUIRE_EQUAL(std::string(name, name + size), exp_name);
        BOOST_REQUIRE_EQUAL(id, base_id + offsets[i]);
        i++;
    }
}

BOOST_AUTO_TEST_CASE(Test_index_4) {
    u64 base_id = 10ul;
    SeriesMatcher matcher(base_id);
    std::vector<std::string> names = {
        "foo tagA=1 tagB=1",
        "foo tagA=1 tagB=2",
        "foo tagA=1 tagB=3",
        "foo tagA=1 tagB=4",
        "foo tagA=2 tagB=1",
        "foo tagA=2 tagB=2",
        "foo tagA=2 tagB=3",
        "foo tagA=2 tagB=4",
    };
    std::vector<u64> ids;
    for (auto name: names) {
        auto id = matcher.add(name.data(), name.data() + name.size());
        if (id == 0) {
            BOOST_FAIL("Bad id");
        }
        ids.push_back(id);
    }
    std::string mname("foo");
    std::map<std::string, std::vector<std::string>> tags = {
        {"tagA", {"2"}},
        {"tagB", {"2", "3"}},
    };

    IncludeMany2Many query(mname, tags);
    auto res = matcher.search(query);
    BOOST_REQUIRE_EQUAL(res.size(), 2);
    int i = 0;
    std::vector<u64> offsets = {
        5,
        6,
    };
    for (auto tup: res) {
        const char* name;
        int size;
        u64 id;
        std::tie(name, size, id) = tup;
        std::string exp_name = names[offsets[i]];
        BOOST_REQUIRE_EQUAL(std::string(name, name + size), exp_name);
        BOOST_REQUIRE_EQUAL(id, base_id + offsets[i]);
        i++;
    }
}
