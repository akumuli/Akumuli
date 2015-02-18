#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "storage.h"


using namespace Akumuli;

struct AkumuliInitializer {
    AkumuliInitializer() {
        aku_initialize(nullptr);
    }
};

void logger_stub(int tag, const char* msg) {}

AkumuliInitializer initializer;

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
    auto window_size = 0xFFFFFFFFFFFF;
    auto threshold = 0xFFFFFF;
    auto cache_size = 0xFFFFFF;
    const char* creation_datetime = "2015-02-03 00:00:00";  // Formatting not required
    db.init_config(threshold, cache_size, window_size, creation_datetime);
    uint32_t actual_threshold, actual_cache_size;
    uint64_t actual_window_size;
    std::string actual_dt;
    db.get_configs(&actual_threshold, &actual_cache_size, &actual_window_size, &actual_dt);
    BOOST_REQUIRE_EQUAL(threshold, actual_threshold);
    BOOST_REQUIRE_EQUAL(cache_size, actual_cache_size);
    BOOST_REQUIRE_EQUAL(window_size, actual_window_size);
    BOOST_REQUIRE_EQUAL(creation_datetime, actual_dt);
}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_schema_1) {

    auto db = MetadataStorage(":memory:", &logger_stub);
    std::vector<std::shared_ptr<SeriesCategory>> cats;
    auto cat = std::make_shared<SeriesCategory>();
    cat->name = "first";
    cat->index_type = AKU_INDEX_BASIC;
    cats.push_back(cat);
    cat = std::make_shared<SeriesCategory>();
    cat->name = "second";
    cat->index_type = AKU_INDEX_BASIC;
    cats.push_back(cat);
    auto schema = std::make_shared<Schema>(cats.begin(), cats.end());
    db.create_schema(schema);
}
