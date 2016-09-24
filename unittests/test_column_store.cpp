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
    return meta;
}

std::shared_ptr<ColumnStore> create_cstore() {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore));
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

BOOST_AUTO_TEST_CASE(Test_column_store_add_values_3) {
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto sessiona = create_session(cstore);
    aku_Sample sample;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    sample.paramid = 111;
    sample.timestamp = 111;
    sample.payload.float64 = 111;
    std::vector<u64> rpoints;
    auto status = sessiona->write(sample, &rpoints);  // series with id 111 doesn't exists
    BOOST_REQUIRE(status == NBTreeAppendResult::FAIL_BAD_ID);
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
    sample.paramid = 42;
    cstore->create_new_column(42);
    std::vector<u64> rpoints;
    session->write(sample, &rpoints);
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

static void fill_data_in(std::shared_ptr<ColumnStore> cstore, std::unique_ptr<CStoreSession>& session, aku_ParamId id, aku_Timestamp begin, aku_Timestamp end) {
    assert(begin < end);
    cstore->create_new_column(id);
    aku_Sample sample;
    sample.paramid = id;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    std::vector<u64> rpoints;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        sample.payload.float64 = ix*0.1;
        sample.timestamp = ix;
        session->write(sample, &rpoints);  // rescue points are ignored now
    }
}

static void test_column_store_query(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_Timestamp> timestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19
    };
    for (auto id: ids) {
        fill_data_in(cstore, session, id, begin, end);
    }

    auto read_ordered_by_series = [&](size_t base_ix, size_t inc) {
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

    auto read_ordered_by_time = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req;
        req.group_by.enabled = false;
        req.select.begin = begin;
        req.select.end = end;
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            req.select.ids.push_back(ids[i]);
        }
        req.order_by = OrderBy::TIME;
        session->query(req, qproc);
        BOOST_REQUIRE_EQUAL(qproc.error, AKU_SUCCESS);
        BOOST_REQUIRE_EQUAL(qproc.samples.size(), ids.size()/inc*timestamps.size());
        size_t niter = 0;
        for (size_t ts = begin; ts < end; ts++) {
            for (size_t i = base_ix; i < ids.size(); i += inc) {
                BOOST_REQUIRE_EQUAL(qproc.samples.at(niter).paramid, ids[i]);
                BOOST_REQUIRE_EQUAL(qproc.samples.at(niter).timestamp, ts);
                niter++;
            }
        }
    };

    read_ordered_by_series(0, ids.size());  // read one series
    read_ordered_by_series(0, 2);  // read even
    read_ordered_by_series(1, 2);  // read odd
    read_ordered_by_series(0, 1);  // read all

    read_ordered_by_time(0, ids.size());  // read one series
    read_ordered_by_time(0, 2);  // read even
    read_ordered_by_time(1, 2);  // read odd
    read_ordered_by_time(0, 1);  // read all
}

BOOST_AUTO_TEST_CASE(Test_column_store_query_2) {
    test_column_store_query(10, 100);
    test_column_store_query(100, 1000);
    test_column_store_query(1000, 100000);
}
