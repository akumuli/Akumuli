#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "metadatastorage.h"
#include "storage2.h"

#include "akumuli.h"
#include "log_iface.h"
#include "status_util.h"

// To initialize apr and sqlite properly
#include <apr.h>
#include <sqlite3.h>

using namespace Akumuli;
using namespace StorageEngine;

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

std::shared_ptr<MetadataStorage> create_metadatastorage() {
    // Create in-memory sqlite database.
    std::shared_ptr<MetadataStorage> meta;
    meta.reset(new MetadataStorage(":memory:"));
    return std::move(meta);
}

std::shared_ptr<ColumnStore> create_cstore() {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore));
    return cstore;
}

std::shared_ptr<Storage> create_storage() {
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto store = std::make_shared<Storage>(meta, bstore, cstore, false);
    return store;
}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_volumes_config) {

    MetadataStorage db(":memory:");
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

    MetadataStorage db(":memory:");
    const char* creation_datetime = "2015-02-03 00:00:00";  // Formatting not required
    db.init_config(creation_datetime);
    std::string actual_dt;
    db.get_configs(&actual_dt);
    BOOST_REQUIRE_EQUAL(creation_datetime, actual_dt);
}

BOOST_AUTO_TEST_CASE(Test_storage_add_series_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto sessiona = store->create_write_session();
    auto sessionb = store->create_write_session();

    aku_Sample samplea;
    status = sessiona->init_series_id(sname, end, &samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    aku_Sample sampleb;
    // Should initialize from global data
    status = sessionb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);

    // Should read local data
    status = sessionb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);
}


BOOST_AUTO_TEST_CASE(Test_storage_add_values_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto sessiona = store->create_write_session();
    auto sessionb = store->create_write_session();

    aku_Sample samplea;
    samplea.payload.type = AKU_PAYLOAD_FLOAT;
    samplea.timestamp = 111;
    samplea.payload.float64 = 111;
    status = sessiona->init_series_id(sname, end, &samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = sessiona->write(samplea);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    aku_Sample sampleb;
    sampleb.payload.type = AKU_PAYLOAD_FLOAT;
    sampleb.timestamp = 222;
    sampleb.payload.float64 = 222;
    // Should initialize from global data
    status = sessionb->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = sessionb->write(sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(samplea.paramid, sampleb.paramid);

    // Should read local data
    sampleb.timestamp = 333;
    sampleb.payload.float64 = 333;
    status = sessiona->init_series_id(sname, end, &sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = sessiona->write(sampleb);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
}


BOOST_AUTO_TEST_CASE(Test_storage_add_values_2) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto sessiona = store->create_write_session();
    {
        auto sessionb = store->create_write_session();

        aku_Sample sample;
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.timestamp = 111;
        sample.payload.float64 = 111;
        status = sessionb->init_series_id(sname, end, &sample);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        status = sessionb->write(sample);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

        // disatcher should be freed and registry entry should be returned
    }
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.timestamp = 222;
    sample.payload.float64 = 222;

    status = sessiona->init_series_id(sname, end, &sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = sessiona->write(sample);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(sample.paramid, sample.paramid);
}
