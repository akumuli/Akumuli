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

AkumuliInitializer initializer;

BOOST_AUTO_TEST_CASE(Test_metadata_storage_volumes_config) {

    auto db = MetadataStorage(":memory:");
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

    auto db = MetadataStorage(":memory:");
    auto window_size = 0xFFFFFFFFFFFF;
    auto threshold = 0xFFFFFF;
    auto cache_size = 0xFFFFFF;
    db.init_config(threshold, cache_size, window_size);
    uint32_t actual_threshold, actual_cache_size;
    uint64_t actual_window_size;
    db.get_configs(&actual_threshold, &actual_cache_size, &actual_window_size);
    BOOST_REQUIRE_EQUAL(threshold, actual_threshold);
    BOOST_REQUIRE_EQUAL(cache_size, actual_cache_size);
    BOOST_REQUIRE_EQUAL(window_size, actual_window_size);
}
