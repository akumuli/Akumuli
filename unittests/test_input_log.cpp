#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>
#include <regex>

#include <apr.h>
#include <sqlite3.h>

#include "akumuli.h"
#include "storage_engine/input_log.h"
#include "log_iface.h"
#include "status_util.h"


struct AprInitializer {
    AprInitializer() {
        apr_initialize();
    }
};

static AprInitializer initializer;

using namespace Akumuli;

BOOST_AUTO_TEST_CASE(Test_input_roundtrip) {
    std::vector<std::tuple<u64, u64, double>> exp, act;
    std::vector<u64> stale_ids;
    {
        InputLog ilog("./", 100, 4096);
        for (int i = 0; i < 10000; i++) {
            ilog.append(42, i, i, &stale_ids);
            exp.push_back(std::make_tuple(42, i, i));
        }
    }
    BOOST_REQUIRE(stale_ids.empty());
    {
        InputLog ilog("./");
        u64 ids[1024];
        u64 tss[1024];
        double xss[1024];
        aku_Status status;
        u32 outsz;
        std::tie(status, outsz) = ilog.read_next(1024, ids, tss, xss);
        for(u32 i = 0; i < outsz; i++) {
            act.push_back(std::make_tuple(ids[i], tss[i], xss[i]));
        }
        ilog.delete_files();
    }
    BOOST_REQUIRE_EQUAL(exp.size(), act.size());
    for (u32 i = 0; i < exp.size(); i++) {
        BOOST_REQUIRE_EQUAL(std::get<0>(exp.at(i)), std::get<0>(act.at(i)));
        BOOST_REQUIRE_EQUAL(std::get<1>(exp.at(i)), std::get<1>(act.at(i)));
        BOOST_REQUIRE_EQUAL(std::get<2>(exp.at(i)), std::get<2>(act.at(i)));
    }
}
