#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <apr.h>

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
        apr_initialize();
        Akumuli::Logger::set_logger(&test_logger);
    }
};

AkumuliInitializer initializer;

void create_tmp_file(const char* file_path, int len) {
    apr_pool_t* pool = NULL;
    apr_file_t* file = NULL;
    apr_status_t status = apr_pool_create(&pool, NULL);
    if (status == APR_SUCCESS) {
        status = apr_file_open(&file, file_path, APR_WRITE|APR_CREATE, APR_OS_DEFAULT, pool);
        if (status == APR_SUCCESS) {
            status = apr_file_trunc(file, len);
            if (status == APR_SUCCESS) {
                status = apr_file_close(file);
            }
        }
    }
    if (pool)
        apr_pool_destroy(pool);
    if (status != APR_SUCCESS) {
        BOOST_FAIL(apr_error_message(status));
    }
}

void delete_tmp_file(const char* file_path) {
    apr_pool_t* pool = NULL;
    apr_pool_create(&pool, NULL);
    apr_file_remove(file_path, pool);
    apr_pool_destroy(pool);
}

BOOST_AUTO_TEST_CASE(TestMmap1)
{
    const char* tmp_file = "testfile";
    delete_tmp_file(tmp_file);
    create_tmp_file(tmp_file, 100);
    MemoryMappedFile mmap(tmp_file, false);
    BOOST_REQUIRE(mmap.is_bad() == false);
    BOOST_REQUIRE(mmap.get_size() == 100);
    delete_tmp_file(tmp_file);
}

BOOST_AUTO_TEST_CASE(TestMmap3)
{
    const char* tmp_file = "testfile";
    delete_tmp_file(tmp_file);
    create_tmp_file(tmp_file, 100);
    {
        MemoryMappedFile mmap(tmp_file, false);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        *begin = 42;
        *end = 24;
    }

    {
        MemoryMappedFile mmap(tmp_file, false);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        BOOST_REQUIRE(*begin == 42);
        BOOST_REQUIRE(*end == 24);
    }

    delete_tmp_file(tmp_file);
}

BOOST_AUTO_TEST_CASE(TestMmap4)
{
    const char* tmp_file = "testfile";
    delete_tmp_file(tmp_file);
    create_tmp_file(tmp_file, 100);
    {
        MemoryMappedFile mmap(tmp_file, false);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        *begin = 42;
        *end = 24;
    }

    {
        MemoryMappedFile mmap(tmp_file, false);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        mmap.remap_file_destructive();
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        BOOST_REQUIRE(*begin != 42);
        BOOST_REQUIRE(*end != 24);
    }

    delete_tmp_file(tmp_file);
}

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
