#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <sqlite3.h>
#include <apr.h>
#include <apr_dbd.h>

#include "akumuli.h"
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"
#include "storage_engine/tree_registry.h"
#include "log_iface.h"


void test_logger(aku_LogLevel tag, const char* msg) {
    AKU_UNUSED(tag);
    BOOST_MESSAGE(msg);
}

struct AkumuliInitializer {
    AkumuliInitializer() {
        sqlite3_initialize();
        apr_initialize();

        apr_pool_t *pool = nullptr;
        auto status = apr_pool_create(&pool, NULL);
        if (status != APR_SUCCESS) {
            AKU_PANIC("Can't create memory pool");
        }
        apr_dbd_init(pool);

        Akumuli::Logger::set_logger(&test_logger);
    }
};

static AkumuliInitializer initializer;

using namespace Akumuli;
using namespace Akumuli::StorageEngine;

std::unique_ptr<MetadataStorage> create_metadatastorage() {
    // Create in-memory sqlite database.
    std::unique_ptr<MetadataStorage> meta;
    meta.reset(new MetadataStorage(":memory:"));
    return std::move(meta);
}

BOOST_AUTO_TEST_CASE(Test_ingress_create) {
    // Do nothing, just create all the things
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));
    auto session = registry->create_session();
}

BOOST_AUTO_TEST_CASE(Test_ingress_add_series_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));
    auto dispa = registry->create_session();
    auto dispb = registry->create_session();

    aku_Sample samplea;
    status = dispa->init_series_id(sname, end, &samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    aku_Sample sampleb;
    // Should initialize from global data
    status = dispb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);

    // Should read local data
    status = dispb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);
}

BOOST_AUTO_TEST_CASE(Test_ingress_add_values_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));
    auto dispa = registry->create_session();
    auto dispb = registry->create_session();

    aku_Sample samplea;
    samplea.payload.type = AKU_PAYLOAD_FLOAT;
    samplea.timestamp = 111;
    samplea.payload.float64 = 111;
    status = dispa->init_series_id(sname, end, &samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = dispa->write(samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    aku_Sample sampleb;
    sampleb.payload.type = AKU_PAYLOAD_FLOAT;
    sampleb.timestamp = 222;
    sampleb.payload.float64 = 222;
    // Should initialize from global data
    status = dispb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = dispb->write(sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);

    // Should read local data
    sampleb.timestamp = 333;
    sampleb.payload.float64 = 333;
    status = dispa->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = dispa->write(sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
}

BOOST_AUTO_TEST_CASE(Test_ingress_add_values_2) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));

    auto dispa = registry->create_session();
    {
        auto dispb = registry->create_session();

        aku_Sample sample;
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.timestamp = 111;
        sample.payload.float64 = 111;
        status = dispb->init_series_id(sname, end, &sample);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        status = dispb->write(sample);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

        // disatcher should be freed and registry entry should be returned
    }
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.timestamp = 222;
    sample.payload.float64 = 222;

    status = dispa->init_series_id(sname, end, &sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = dispa->write(sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(sample.paramid, sample.paramid);
}

BOOST_AUTO_TEST_CASE(Test_ingress_add_values_3) {
    aku_Status status;
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));
    auto disp = registry->create_session();
    auto dispb = registry->create_session();

    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.paramid = 111;
    sample.timestamp = 111;
    sample.payload.float64 = 111;
    status = disp->write(sample);  // series with id 111 doesn't exists
    BOOST_REQUIRE_NE(status, AKU_SUCCESS);
}

BOOST_AUTO_TEST_CASE(Test_read_values_back_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(bstore, std::move(meta));
    auto session = registry->create_session();

    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.timestamp = 111;
    sample.payload.float64 = 111;
    status = session->init_series_id(sname, end, &sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = session->write(sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    boost::property_tree::ptree ptree;
    ptree.put("begin", "0");
    ptree.put("end", "200");
    ptree.put("filter", ".+");
    std::unique_ptr<ConcatCursor> cursor;
    std::tie(status, cursor) = std::move(session->query(ptree));
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    aku_Sample out;
    size_t outsize;
    std::tie(status, outsize) = cursor->read(&out, 1);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(outsize, 1);
    BOOST_REQUIRE_EQUAL(out.timestamp, sample.timestamp);
    BOOST_REQUIRE_EQUAL(out.paramid, sample.paramid);
    BOOST_REQUIRE_EQUAL(out.payload.float64, sample.payload.float64);
}
