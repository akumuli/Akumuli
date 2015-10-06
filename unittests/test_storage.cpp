#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "metadatastorage.h"


using namespace Akumuli;

void logger_stub(aku_LogLevel tag, const char* msg) {}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_volumes_config) {

    auto db = MetadataStorage(":memory:", &logger_stub);
    std::vector<MetadataStorage::VolumeDesc> volumes = {
        std::make_pair(0, "first"),
        std::make_pair(1, "second"),
        std::make_pair(2, "third"),
    };
    db.init_volumes(volumes);
    auto actual = db.get_volumes();
    for (int i = 0; i < 3; i++) {
        BOOST_REQUIRE_EQUAL(volumes.at(i).first, actual.at(i).first);
        BOOST_REQUIRE_EQUAL(volumes.at(i).second, actual.at(i).second);
    }
}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_numeric_config) {

    auto db = MetadataStorage(":memory:", &logger_stub);
    const char* creation_datetime = "2015-02-03 00:00:00";  // Formatting not required
    db.init_config(creation_datetime);
    std::string actual_dt;
    db.get_configs(&actual_dt);
    BOOST_REQUIRE_EQUAL(creation_datetime, actual_dt);
}

