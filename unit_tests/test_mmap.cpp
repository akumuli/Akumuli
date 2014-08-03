#include <iostream>

#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <apr.h>

#include "akumuli_def.h"
#include "util.h"

using namespace Akumuli;

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
    MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
    BOOST_REQUIRE(mmap.is_bad() == false);
    BOOST_REQUIRE(mmap.get_size() == 100);
    delete_tmp_file(tmp_file);
}

BOOST_AUTO_TEST_CASE(TestMmap2)
{
    const char* tmp_file = "file_that_doesnt_exist";
    delete_tmp_file(tmp_file);
    MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
    BOOST_REQUIRE(mmap.is_bad() == true);
    bool throw_works = false;
    try {
        mmap.panic_if_bad();
    }
    catch(AprException const&) {
        throw_works = true;
    }
    BOOST_REQUIRE(throw_works);
}

BOOST_AUTO_TEST_CASE(TestMmap3)
{
    const char* tmp_file = "testfile";
    delete_tmp_file(tmp_file);
    create_tmp_file(tmp_file, 100);
    {
        MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        *begin = 42;
        *end = 24;
    }

    {
        MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
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
        MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
        BOOST_REQUIRE(mmap.is_bad() == false);
        BOOST_REQUIRE(mmap.get_size() == 100);
        char* begin = (char*)mmap.get_pointer();
        char* end = begin + 99;
        *begin = 42;
        *end = 24;
    }

    {
        MemoryMappedFile mmap(tmp_file, 0, &aku_console_logger);
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
