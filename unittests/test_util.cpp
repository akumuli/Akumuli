#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include "akumuli_def.h"
#include "util.h"
#include "crc32c.h"
#include "log_iface.h"

using namespace Akumuli;

void test_logger(aku_LogLevel tag, const char* msg) {
    BOOST_TEST_MESSAGE(msg);
}

struct AkumuliInitializer {
    AkumuliInitializer() {
        Akumuli::Logger::set_logger(&test_logger);
    }
};

AkumuliInitializer initializer;

BOOST_AUTO_TEST_CASE(test_crc32c_0) {
    auto crc32hw = chose_crc32c_implementation(CRC32C_hint::DETECT);
    auto crc32sw = chose_crc32c_implementation(CRC32C_hint::FORCE_SW);
    if (crc32hw == crc32sw) {
        BOOST_TEST_MESSAGE("Can't compare crc32c implementation, hardware version is not available.");
        return;
    }
    auto gen = []() {
        return static_cast<u8>(rand());
    };
    std::vector<u8> data(111111, 0);
    std::generate(data.begin(), data.end(), gen);

    u32 hw = 0, sw = 0;
    hw = crc32hw(hw, data.data(), data.size());
    sw = crc32sw(sw, data.data(), data.size());

    BOOST_REQUIRE_EQUAL(hw, sw);
}
