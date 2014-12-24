#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <iostream>

#include "expected.h"

using namespace Akumuli;

static int exception_dtor_calls = 0;
static int exception_ctor_calls = 0;

struct Exception : std::exception {
    int tag;
    Exception(int t = 0) : tag(t) { exception_ctor_calls++; }
    Exception(Exception const& e) : tag(e.tag) { exception_ctor_calls++; }
    Exception& operator = (Exception const& e) {
        if (&e != this) {
            tag = e.tag;
        }
        return *this;
    }
    ~Exception() { exception_dtor_calls++; }
};

BOOST_AUTO_TEST_CASE(Test_expected_normal) {

    Expected<std::string> value("hello");
    BOOST_REQUIRE_NO_THROW(value.get());
    BOOST_REQUIRE_EQUAL(value.get(), std::string("hello"));
}

BOOST_AUTO_TEST_CASE(Test_expected_error) {
    Exception err;
    Expected<std::string> value(std::make_exception_ptr(err));
    BOOST_REQUIRE(!value.ok());
    BOOST_CHECK_THROW(value.get(), Exception);
}

BOOST_AUTO_TEST_CASE(Test_expected_unpack) {
    Exception err, err2;
    Expected<std::string> value(std::make_exception_ptr(err));
    bool unpacked = false;
    BOOST_REQUIRE_NO_THROW(unpacked = value.unpack_error(&err2));
    BOOST_REQUIRE(unpacked);
}

BOOST_AUTO_TEST_CASE(Test_expected_exception_lifetime) {
    int cted = exception_ctor_calls;
    int dted = exception_dtor_calls;
    BOOST_REQUIRE_EQUAL(cted, dted);
    {
        Exception err, err2;
        Expected<std::string> value(std::make_exception_ptr(err));
        bool unpacked = false;
        BOOST_REQUIRE_NO_THROW(unpacked = value.unpack_error(&err2));
        BOOST_REQUIRE(unpacked);
    }
    int cted2 = exception_ctor_calls;
    int dted2 = exception_dtor_calls;
    BOOST_REQUIRE_EQUAL(cted2, dted2);

}

BOOST_AUTO_TEST_CASE(Test_expected_value_lifetime) {
    int cted = exception_ctor_calls;
    int dted = exception_dtor_calls;
    BOOST_REQUIRE_EQUAL(cted, dted);
    {
        Exception v1, v2, v4;
        Expected<Exception> value(v1);
        Expected<Exception> value2(v2), value4(v4);
        value = value2;
        BOOST_REQUIRE_NO_THROW(v2 = value4.get());
    }
    int cted2 = exception_ctor_calls;
    int dted2 = exception_dtor_calls;
    BOOST_REQUIRE_EQUAL(cted2, dted2);

}

