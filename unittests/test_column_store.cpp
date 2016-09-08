#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>

#include <apr.h>
#include <sqlite3.h>

#include "akumuli.h"
#include "storage_engine/column_store.h"
#include "log_iface.h"
#include "status_util.h"

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

std::shared_ptr<ColumnStore> create_cstore() {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore, create_metadatastorage()));
    return cstore;
}

std::unique_ptr<CStoreSession> create_session(std::shared_ptr<ColumnStore> cstore) {
    std::unique_ptr<CStoreSession> session;
    session.reset(new CStoreSession(cstore));
    return session;
}

BOOST_AUTO_TEST_CASE(Test_columns_store_create_1) {
    std::shared_ptr<ColumnStore> cstore = create_cstore();
    std::unique_ptr<CStoreSession> session = create_session(cstore);
}

BOOST_AUTO_TEST_CASE(Test_column_store_add_series_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto sessiona = create_session(cstore);
    auto sessionb = create_session(cstore);

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

BOOST_AUTO_TEST_CASE(Test_column_store_add_values_1) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto sessiona = create_session(cstore);
    auto sessionb = create_session(cstore);

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


BOOST_AUTO_TEST_CASE(Test_column_store_add_values_2) {
    aku_Status status;
    const char* sname = "hello world=1";
    const char* end = sname + strlen(sname);

    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto sessiona = create_session(cstore);
    {
        auto sessionb = create_session(cstore);

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

BOOST_AUTO_TEST_CASE(Test_column_store_add_values_3) {
    aku_Status status;
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto sessiona = create_session(cstore);
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.paramid = 111;
    sample.timestamp = 111;
    sample.payload.float64 = 111;
    status = sessiona->write(sample);  // series with id 111 doesn't exists
    BOOST_REQUIRE_NE(status, AKU_SUCCESS);
}

struct QueryProcessorMock : QP::IQueryProcessor {
    bool started = false;
    bool stopped = false;
    std::vector<aku_Sample> samples;
    aku_Status error = AKU_SUCCESS;

    virtual QP::QueryRange range() const override {
        throw "not implemented";
    }
    virtual QP::IQueryFilter &filter() override {
        throw "not implemented";
    }
    virtual SeriesMatcher *matcher() override {
        return nullptr;
    }
    virtual bool start() override {
        started = true;
        return true;
    }
    virtual void stop() override {
        stopped = true;
    }
    virtual bool put(const aku_Sample &sample) override {
        samples.push_back(sample);
        return true;
    }
    virtual void set_error(aku_Status err) override {
        error = err;
    }
};

BOOST_AUTO_TEST_CASE(Test_column_store_query_1) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    QueryProcessorMock qproc;
    aku_Sample sample;
    sample.timestamp = 42;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    const char* begin = "test tag=val";
    const char* end = begin + strlen(begin);
    session->init_series_id(begin, end, &sample);
    session->write(sample);
    ReshapeRequest req;
    req.group_by.enabled = false;
    req.select.begin = 0;
    req.select.end = 100;
    req.select.ids.push_back(sample.paramid);
    req.order_by = OrderBy::SERIES;
    session->query(req, qproc);
    BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
    BOOST_REQUIRE(qproc.samples.size() == 1);
    BOOST_REQUIRE(qproc.samples.at(0).paramid == sample.paramid);
    BOOST_REQUIRE(qproc.samples.at(0).timestamp == sample.timestamp);
}

static void fill_data_in(std::unique_ptr<CStoreSession>& session, aku_ParamId id, aku_Timestamp begin, aku_Timestamp end) {
    assert(begin < end);
    aku_Sample sample;
    sample.paramid = id;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        sample.payload.float64 = ix*0.1;
        sample.timestamp = ix;
        session->write(sample);
    }
}

static std::vector<aku_ParamId> init_series_ids(std::vector<std::string> const& names, std::unique_ptr<CStoreSession>& session) {
    std::vector<aku_ParamId> ids;
    for (auto name: names) {
        aku_Sample sample;
        auto status = session->init_series_id(name.data(), name.data() + name.size(), &sample);
        BOOST_REQUIRE(status == AKU_SUCCESS);
        ids.push_back(sample.paramid);
    }
    return ids;
}

static void test_column_store_query(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_Timestamp> timestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    std::vector<std::string> names = {
        "m t=0",
        "m t=1",
        "m t=2",
        "m t=3",
        "m t=4",
        "m t=5",
        "m t=6",
        "m t=7",
        "m t=8",
        "m t=9",
    };
    auto ids = init_series_ids(names, session);
    for (auto id: ids) {
        fill_data_in(session, id, begin, end);
    }

    auto read_fn = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req;
        req.group_by.enabled = false;
        req.select.begin = begin;
        req.select.end = end;
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            req.select.ids.push_back(ids[i]);
        }
        req.order_by = OrderBy::SERIES;
        session->query(req, qproc);
        BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
        BOOST_REQUIRE(qproc.samples.size() == ids.size()/inc*timestamps.size());
        size_t niter = 0;
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            size_t baseix = timestamps.size() * niter;
            for (size_t tx = begin; tx < end; tx++) {
                BOOST_REQUIRE(qproc.samples.at(tx - begin + baseix).paramid == ids[i]);
                BOOST_REQUIRE(qproc.samples.at(tx - begin + baseix).timestamp == tx);
            }
            niter++;
        }
    };

    read_fn(0, ids.size());  // read one series
    read_fn(0, 2);  // read even
    read_fn(1, 2);  // read odd
    read_fn(0, 1);  // read all
}

BOOST_AUTO_TEST_CASE(Test_column_store_query_2) {
    test_column_store_query(10, 100);
    test_column_store_query(100, 1000);
    test_column_store_query(1000, 100000);
}
