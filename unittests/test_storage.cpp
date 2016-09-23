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

static AkumuliInitializer initializer;

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

// Test read queries

void fill_data(std::shared_ptr<StorageSession> session, aku_Timestamp begin, aku_Timestamp end, std::vector<std::string> const& names) {
    for (aku_Timestamp ts = begin; ts < end; ts++) {
        for (auto it: names) {
            aku_Sample sample;
            sample.timestamp = ts;
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.payload.float64 = double(ts)/10.0;
            auto status = session->init_series_id(it.data(), it.data() + it.size(), &sample);
            if (status != AKU_SUCCESS) {
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
            }
            status = session->write(sample);
            if (status != AKU_SUCCESS) {
                BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
            }
        }
    }
}

struct CursorMock : InternalCursor {
    bool done;
    std::vector<aku_Sample> samples;
    aku_Status error;

    CursorMock() {
        done = false;
        error = AKU_SUCCESS;
    }

    virtual bool put(Caller&, const aku_Sample &val) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        samples.push_back(val);
        return true;
    }

    virtual void complete(Caller&) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        done = true;
    }

    virtual void set_error(Caller &, aku_Status error_code) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        done = true;
        error = error_code;
    }
};

std::string make_scan_query(aku_Timestamp begin, aku_Timestamp end, OrderBy order) {
    std::stringstream str;
    str << "{ \"range\": { \"from\": " << begin << ", \"to\": " << end << "},";
    str << "  \"order-by\": " << (order == OrderBy::SERIES ? "\"series\"" : "\"time\"");
    str << "}";
    return str.str();
}

void check_timestamps(CursorMock const& mock, aku_Timestamp begin, aku_Timestamp end, OrderBy order, size_t nseries) {
    std::vector<aku_Timestamp> expected;
    if (begin < end) {
        for (aku_Timestamp ts = begin; ts < end; ts++) {
            expected.push_back(ts);
        }
    } else {
        for (aku_Timestamp ts = begin-1; ts > end; ts--) {
            expected.push_back(ts);
        }
    }
    size_t tsix = 0;
    if (order == OrderBy::SERIES) {
        for (size_t s = 0; s < nseries; s++) {
            for (auto expts: expected) {
                BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix++).timestamp);
            }
        }
        BOOST_REQUIRE_EQUAL(tsix, mock.samples.size());
    } else {
        for (auto expts: expected) {
            for (size_t s = 0; s < nseries; s++) {
                BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix++).timestamp);
            }
        }
        BOOST_REQUIRE_EQUAL(tsix, mock.samples.size());
    }
}

void test_storage_read_query(aku_Timestamp begin, aku_Timestamp end, OrderBy order) {
    std::vector<std::string> series_names = {
        "test key=0",
        "test key=1",
        "test key=2",
        "test key=3",
        "test key=4",
        "test key=5",
        "test key=6",
        "test key=7",
        "test key=8",
        "test key=9",
    };
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, std::min(begin, end), std::max(begin, end), series_names);
    Caller caller;
    CursorMock cursor;
    auto query = make_scan_query(begin, end, order);
    session->query(caller, &cursor, query.c_str());
    BOOST_REQUIRE(cursor.done);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    size_t expected_size;
    if (begin < end) {
        expected_size = (end - begin)*series_names.size();
    } else {
        // because we will read data in (end, begin] range but
        // fill data in [end, begin) range
        expected_size = (begin - end - 1)*series_names.size();
    }
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_size);
    check_timestamps(cursor, begin, end, order, series_names.size());
}


BOOST_AUTO_TEST_CASE(Test_storage_query) {
    std::vector<std::tuple<aku_Timestamp, aku_Timestamp, OrderBy>> input = {
        std::make_tuple( 100ul,  200ul, OrderBy::TIME),
        std::make_tuple( 200ul,  100ul, OrderBy::TIME),
        std::make_tuple(1000ul, 2000ul, OrderBy::TIME),
        std::make_tuple(2000ul, 1000ul, OrderBy::TIME),
        std::make_tuple( 100ul,  200ul, OrderBy::SERIES),
        std::make_tuple( 200ul,  100ul, OrderBy::SERIES),
        std::make_tuple(1000ul, 2000ul, OrderBy::SERIES),
        std::make_tuple(2000ul, 1000ul, OrderBy::SERIES),
    };
    for (auto tup: input) {
        OrderBy order;
        aku_Timestamp begin;
        aku_Timestamp end;
        std::tie(begin, end, order) = tup;
        test_storage_read_query(begin, end, order);
    }
}

// Test reopen

