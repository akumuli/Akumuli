#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>

#include <apr.h>
#include <sqlite3.h>

#include "akumuli.h"
#include "storage_engine/column_store.h"
#include "query_processing/queryplan.h"
#include "log_iface.h"
#include "status_util.h"

void test_logger(aku_LogLevel tag, const char* msg) {
    AKU_UNUSED(tag);
    BOOST_TEST_MESSAGE(msg);
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
using namespace Akumuli::QP;

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

struct QueryProcessorMock : QP::IStreamProcessor {
    bool started = false;
    bool stopped = false;
    std::vector<aku_Sample> samples;
    aku_Status error = AKU_SUCCESS;

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


void execute(std::shared_ptr<ColumnStore> cstore, IStreamProcessor* proc, ReshapeRequest const& req) {
    aku_Status status;
    std::unique_ptr<QP::IQueryPlan> query_plan;
    std::tie(status, query_plan) = QP::QueryPlanBuilder::create(req);
    if (status != AKU_SUCCESS) {
        throw std::runtime_error("Can't create query plan");
    }
    if (proc->start()) {
        QueryPlanExecutor executor;
        executor.execute(*cstore, std::move(query_plan), *proc);
        proc->stop();
    }
}

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
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = 0;
    req.select.end = 100;
    req.select.columns.emplace_back();
    req.select.columns[0].ids.push_back(sample.paramid);
    req.order_by = OrderBy::SERIES;
    execute(cstore, &qproc, req);
    BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
    BOOST_REQUIRE(qproc.samples.size() == 1);
    BOOST_REQUIRE(qproc.samples.at(0).paramid == sample.paramid);
    BOOST_REQUIRE(qproc.samples.at(0).timestamp == sample.timestamp);
}

static double fill_data_in(std::shared_ptr<ColumnStore> cstore, std::unique_ptr<CStoreSession>& session, aku_ParamId id, aku_Timestamp begin, aku_Timestamp end) {
    assert(begin < end);
    cstore->create_new_column(id);
    aku_Sample sample;
    sample.paramid = id;
    sample.payload.type = AKU_PAYLOAD_FLOAT;
    std::vector<u64> rpoints;
    double sum = 0;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        sample.payload.float64 = ix*0.1;
        sample.timestamp = ix;
        session->write(sample, &rpoints);  // rescue points are ignored now
        sum += sample.payload.float64;
    }
    return sum;
}

static void test_column_store_query(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_Timestamp> timestamps, invtimestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    std::copy(timestamps.rbegin(), timestamps.rend(), std::back_inserter(invtimestamps));
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<aku_ParamId> invids;
    std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));
    for (auto id: ids) {
        fill_data_in(cstore, session, id, begin, end);
    }

    // Read in series order in forward direction
    auto read_ordered_by_series = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = false;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.emplace_back();
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            req.select.columns[0].ids.push_back(ids[i]);
        }
        req.order_by = OrderBy::SERIES;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
        BOOST_REQUIRE(qproc.samples.size() == ids.size()/inc*timestamps.size());
        size_t niter = 0;
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            for (auto tx: timestamps) {
                BOOST_REQUIRE(qproc.samples.at(niter).paramid == ids[i]);
                BOOST_REQUIRE(qproc.samples.at(niter).timestamp == tx);
                niter++;
            }
        }
    };

    // Read in series order in backward direction
    auto inv_read_ordered_by_series = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = false;
        req.select.begin = end;
        req.select.end = begin-1; // we need to read data in range (begin-1, end] to hit value with `begin` timestamp
        req.select.columns.emplace_back();
        for(size_t i = base_ix; i < invids.size(); i += inc) {
            req.select.columns[0].ids.push_back(invids[i]);
        }
        req.order_by = OrderBy::SERIES;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
        BOOST_REQUIRE(qproc.samples.size() == invids.size()/inc*invtimestamps.size());
        size_t niter = 0;
        for(size_t i = base_ix; i < invids.size(); i += inc) {
            for (auto ts: invtimestamps) {
                BOOST_REQUIRE(qproc.samples.at(niter).paramid == invids[i]);
                BOOST_REQUIRE(qproc.samples.at(niter).timestamp == ts);
                niter++;
            }
        }
    };

    // Read in time order in forward direction
    auto read_ordered_by_time = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = false;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.emplace_back();
        for(size_t i = base_ix; i < ids.size(); i += inc) {
            req.select.columns[0].ids.push_back(ids[i]);
        }
        req.order_by = OrderBy::TIME;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE_EQUAL(qproc.error, AKU_SUCCESS);
        BOOST_REQUIRE_EQUAL(qproc.samples.size(), ids.size()/inc*timestamps.size());
        size_t niter = 0;
        for (size_t ts = begin; ts < end; ts++) {
            for (size_t i = base_ix; i < ids.size(); i += inc) {
                if (qproc.samples.at(niter).paramid != ids[i]) {
                    BOOST_REQUIRE_EQUAL(qproc.samples.at(niter).paramid, ids[i]);
                }
                if (qproc.samples.at(niter).timestamp != ts) {
                    BOOST_REQUIRE_EQUAL(qproc.samples.at(niter).timestamp, ts);
                }
                niter++;
            }
        }
    };

    // Read in time order in backward direction
    auto inv_read_ordered_by_time = [&](size_t base_ix, size_t inc) {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = false;
        req.select.begin = end;
        req.select.end = begin - 1;
        req.select.columns.emplace_back();
        for(size_t i = base_ix; i < invids.size(); i += inc) {
            req.select.columns[0].ids.push_back(invids[i]);
        }
        req.order_by = OrderBy::TIME;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE_EQUAL(qproc.error, AKU_SUCCESS);
        BOOST_REQUIRE_EQUAL(qproc.samples.size(), invids.size()/inc*invtimestamps.size());
        size_t niter = 0;
        for (auto ts: invtimestamps) {
            for (size_t i = base_ix; i < invids.size(); i += inc) {
                BOOST_REQUIRE_EQUAL(qproc.samples.at(niter).paramid, invids[i]);
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

    inv_read_ordered_by_series(0, ids.size());  // read one series
    inv_read_ordered_by_series(0, 2);  // read even
    inv_read_ordered_by_series(1, 2);  // read odd
    inv_read_ordered_by_series(0, 1);  // read all

    inv_read_ordered_by_time(0, ids.size());  // read one series
    inv_read_ordered_by_time(0, 2);  // read even
    inv_read_ordered_by_time(1, 2);  // read odd
    inv_read_ordered_by_time(0, 1);  // read all
}

BOOST_AUTO_TEST_CASE(Test_column_store_query_2) {
    test_column_store_query(10, 100);
    test_column_store_query(100, 1000);
    test_column_store_query(1000, 100000);
}

void test_groupby_query() {
    const aku_Timestamp begin = 100;
    const aku_Timestamp end  = 1100;
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_Timestamp> timestamps, invtimestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    std::copy(timestamps.rbegin(), timestamps.rend(), std::back_inserter(invtimestamps));
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19,
        20,21,22,23,24,25,26,27,28,29,
    };
    std::unordered_map<aku_ParamId, aku_ParamId> translation_table;
    for (auto id: ids) {
        if (id < 20) {
            translation_table[id] = 1;
        } else {
            translation_table[id] = 2;
        }
    }
    std::shared_ptr<PlainSeriesMatcher> matcher = std::make_shared<PlainSeriesMatcher>();
    matcher->_add("_ten_", 1);
    matcher->_add("_twenty_", 2);
    std::vector<aku_ParamId> invids;
    std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));
    for (auto id: ids) {
        fill_data_in(cstore, session, id, begin, end);
    }

    // Read in series order in forward direction
    auto read_ordered_by_series = [&]() {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = true;
        req.group_by.transient_map = translation_table;
        req.select.matcher = matcher;
        req.select.begin = begin;
        req.select.end = 1 + end;
        req.select.columns.emplace_back();
        req.select.columns.at(0).ids = ids;
        req.order_by = OrderBy::SERIES;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
        BOOST_REQUIRE(qproc.samples.size() == timestamps.size()*ids.size());
        size_t niter = 0;
        for(size_t id = 1; id < 3; id++) {
            for (auto tx: timestamps) {
                for (size_t k = 0; k < 10; k++) {
                    BOOST_REQUIRE(qproc.samples.at(niter).paramid == id);
                    BOOST_REQUIRE(qproc.samples.at(niter).timestamp == tx);
                    niter++;
                }
            }
        }
    };

    // Read in series order in forward direction
    auto read_ordered_by_time = [&]() {
        QueryProcessorMock qproc;
        ReshapeRequest req = {};
        req.group_by.enabled = true;
        req.group_by.transient_map = translation_table;
        req.select.matcher = matcher;
        req.select.begin = begin;
        req.select.end = 1 + end;
        req.select.columns.emplace_back();
        req.select.columns.at(0).ids = ids;
        req.order_by = OrderBy::TIME;
        execute(cstore, &qproc, req);
        BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
        BOOST_REQUIRE(qproc.samples.size() == timestamps.size()*ids.size());
        size_t niter = 0;
        for (auto tx: timestamps) {
            for(size_t id = 1; id < 3; id++) {
                for (size_t k = 0; k < 10; k++) {
                    BOOST_REQUIRE(qproc.samples.at(niter).paramid == id);
                    BOOST_REQUIRE(qproc.samples.at(niter).timestamp == tx);
                    niter++;
                }
            }
        }
    };

    read_ordered_by_series();
    read_ordered_by_time();
}

BOOST_AUTO_TEST_CASE(Test_column_store_group_by_1) {
    test_groupby_query();
}

void test_reopen(aku_Timestamp begin, aku_Timestamp end) {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore));
    auto session = create_session(cstore);
    std::vector<aku_Timestamp> timestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<aku_ParamId> invids;
    std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));

    for (auto id: ids) {
        fill_data_in(cstore, session, id, begin, end);
    }

    session.reset();
    auto mapping = cstore->close();

    // Reopen
    cstore.reset(new ColumnStore(bstore));
    cstore->open_or_restore(mapping);
    session = create_session(cstore);

    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.emplace_back();
    for(size_t i = 0; i < ids.size(); i++) {
        req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::SERIES;
    execute(cstore, &qproc, req);

    // Check everything
    BOOST_REQUIRE(qproc.error == AKU_SUCCESS);
    BOOST_REQUIRE(qproc.samples.size() == ids.size()*timestamps.size());
    size_t niter = 0;
    for(size_t i = 0; i < ids.size(); i++) {
        for (auto tx: timestamps) {
            BOOST_REQUIRE(qproc.samples.at(niter).paramid == ids[i]);
            BOOST_REQUIRE(qproc.samples.at(niter).timestamp == tx);
            niter++;
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_column_store_reopen_1) {
    test_reopen(100, 200);     // 100 el.
}

BOOST_AUTO_TEST_CASE(Test_column_store_reopen_2) {
    test_reopen(1000, 2000);   // 1000 el.
}

BOOST_AUTO_TEST_CASE(Test_column_store_reopen_3) {
    test_reopen(1000, 11000);  // 10000 el.
}

void test_aggregation(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<double> sums;
    for (auto id: ids) {
        double sum = fill_data_in(cstore, session, id, begin, end);
        sums.push_back(sum);
    }
    QueryProcessorMock mock;
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.func = { AggregationFunction::SUM };
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({ids});

    execute(cstore, &mock, req);

    BOOST_REQUIRE_EQUAL(mock.samples.size(), ids.size());
    for (auto i = 0u; i < mock.samples.size(); i++) {
        BOOST_REQUIRE_EQUAL(mock.samples.at(i).paramid, ids.at(i));
        BOOST_REQUIRE_CLOSE(mock.samples.at(i).payload.float64, sums.at(i), 10E-5);
    }
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregation_1) {
    test_aggregation(100, 1100);
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregation_2) {
    test_aggregation(1000, 11000);
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregation_3) {
    test_aggregation(10000, 110000);
}

void test_aggregation_group_by(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19,
        20,21,22,23,24,25,26,27,28,29
    };
    std::unordered_map<aku_ParamId, aku_ParamId> translation_table;
    for (auto id: ids) {
        if (id < 20) {
            translation_table[id] = 1;
        } else {
            translation_table[id] = 2;
        }
    }
    std::shared_ptr<PlainSeriesMatcher> matcher = std::make_shared<PlainSeriesMatcher>();
    matcher->_add("_ten_", 1);
    matcher->_add("_twenty_", 2);
    double sum1 = 0, sum2 = 0;
    for (auto id: ids) {
        double sum = fill_data_in(cstore, session, id, begin, end);
        if (id < 20) {
            sum1 += sum;
        } else {
            sum2 += sum;
        }
    }
    QueryProcessorMock mock;
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.func = { AggregationFunction::SUM };
    req.group_by.enabled = true;
    req.select.matcher = matcher;
    req.group_by.transient_map = translation_table;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({ids});

    execute(cstore, &mock, req);

    std::vector<double> sums = {sum1, sum2};
    ids = {1, 2};

    BOOST_REQUIRE_EQUAL(mock.samples.size(), ids.size());
    for (auto i = 0u; i < mock.samples.size(); i++) {
        BOOST_REQUIRE_EQUAL(mock.samples.at(i).paramid, ids.at(i));
        BOOST_REQUIRE_CLOSE(mock.samples.at(i).payload.float64, sums.at(i), 10E-5);
    }
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregation_group_by_1) {
    test_aggregation_group_by(100, 1100);
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregation_group_by_2) {
    test_aggregation_group_by(1000, 11000);
}

struct TupleQueryProcessorMock : QP::IStreamProcessor {
    bool started = false;
    bool stopped = false;
    std::vector<u64> bitmaps;
    std::vector<u64> paramids;
    std::vector<u64> timestamps;
    std::vector<std::vector<double>> columns;
    aku_Status error = AKU_SUCCESS;

    TupleQueryProcessorMock(u32 ncol) {
        columns.resize(ncol);
    }

    virtual bool start() override {
        started = true;
        return true;
    }
    virtual void stop() override {
        stopped = true;
    }
    virtual bool put(const aku_Sample &sample) override {
        if ((sample.payload.type & AKU_PAYLOAD_TUPLE) != AKU_PAYLOAD_TUPLE) {
            BOOST_FAIL("Tuple expected");
        }
        union {
            double d;
            u64    u;
        } bitmap;
        bitmap.d = sample.payload.float64;
        bitmaps.push_back(bitmap.u);
        paramids.push_back(sample.paramid);
        timestamps.push_back(sample.timestamp);
        double const* tup = reinterpret_cast<double const*>(sample.payload.data);
        for (auto i = 0u; i < columns.size(); i++) {
            BOOST_REQUIRE((bitmap.u & (1 << i)) != 0);
            columns.at(i).push_back(tup[i]);
        }
        return true;
    }
    virtual void set_error(aku_Status err) override {
        error = err;
    }
};

void test_join(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_ParamId> col1 = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<aku_ParamId> col2 = {
        20,21,22,23,24,25,26,27,28,29
    };
    std::vector<aku_Timestamp> timestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    for (auto id: col1) {
        fill_data_in(cstore, session, id, begin, end);
    }
    for (auto id: col2) {
        fill_data_in(cstore, session, id, begin, end);
    }

    {
        TupleQueryProcessorMock mock(2);
        ReshapeRequest req = {};
        req.agg.enabled = false;
        req.group_by.enabled = false;
        req.order_by = OrderBy::SERIES;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.push_back({col1});
        req.select.columns.push_back({col2});

        execute(cstore, &mock, req);

        BOOST_REQUIRE(mock.error == AKU_SUCCESS);
        u32 ix = 0;
        for (auto id: col1) {
            for (auto ts: timestamps) {
                BOOST_REQUIRE(mock.paramids.at(ix) == id);
                BOOST_REQUIRE(mock.timestamps.at(ix) == ts);
                double expected = ts*0.1;
                double col0 = mock.columns[0][ix];
                double col1 = mock.columns[1][ix];
                BOOST_REQUIRE_CLOSE(expected, col0, 10E-10);
                BOOST_REQUIRE_CLOSE(col0, col1, 10E-10);
                ix++;
            }
        }
    }

    {
        TupleQueryProcessorMock mock(2);
        ReshapeRequest req = {};
        req.agg.enabled = false;
        req.group_by.enabled = false;
        req.order_by = OrderBy::TIME;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.push_back({col1});
        req.select.columns.push_back({col2});

        execute(cstore, &mock, req);

        BOOST_REQUIRE(mock.error == AKU_SUCCESS);
        u32 ix = 0;
        for (auto ts: timestamps) {
            for (auto id: col1) {
                BOOST_REQUIRE(mock.paramids.at(ix) == id);
                BOOST_REQUIRE(mock.timestamps.at(ix) == ts);
                double expected = ts*0.1;
                double col0 = mock.columns[0][ix];
                double col1 = mock.columns[1][ix];
                BOOST_REQUIRE_CLOSE(expected, col0, 10E-10);
                BOOST_REQUIRE_CLOSE(col0, col1, 10E-10);
                ix++;
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_column_store_join_1) {
    test_join(100, 1100);
}

void test_group_aggregate(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_ParamId> col = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<aku_Timestamp> timestamps;
    for (aku_Timestamp ix = begin; ix < end; ix++) {
        timestamps.push_back(ix);
    }
    for (auto id: col) {
        fill_data_in(cstore, session, id, begin, end);
    }

    auto test_series_order = [&](size_t step)
    {
        std::vector<aku_Timestamp> model_timestamps;
        for (size_t i = 0u; i < timestamps.size(); i += step) {
            model_timestamps.push_back(timestamps.at(i));
        }
        TupleQueryProcessorMock mock(1);
        ReshapeRequest req = {};
        req.agg.enabled = true;
        req.agg.step = step;
        req.agg.func = { AggregationFunction::MIN };
        req.group_by.enabled = false;
        req.order_by = OrderBy::SERIES;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.push_back({col});

        execute(cstore, &mock, req);

        BOOST_REQUIRE(mock.error == AKU_SUCCESS);
        u32 ix = 0;
        for (auto id: col) {
            for (auto ts: model_timestamps) {
                BOOST_REQUIRE(mock.paramids.at(ix) == id);
                BOOST_REQUIRE(mock.timestamps.at(ix) == ts);
                double expected = ts*0.1;
                double xs = mock.columns[0][ix];
                BOOST_REQUIRE_CLOSE(expected, xs, 10E-10);
                ix++;
            }
        }
        BOOST_REQUIRE(ix != 0);
    };
    auto test_time_order = [&](size_t step)
    {
        std::vector<aku_Timestamp> model_timestamps;
        for (size_t i = 0u; i < timestamps.size(); i += step) {
            model_timestamps.push_back(timestamps.at(i));
        }
        TupleQueryProcessorMock mock(1);
        ReshapeRequest req = {};
        req.agg.enabled = true;
        req.agg.step = step;
        req.agg.func = { AggregationFunction::MIN };
        req.group_by.enabled = false;
        req.order_by = OrderBy::TIME;
        req.select.begin = begin;
        req.select.end = end;
        req.select.columns.push_back({col});

        execute(cstore, &mock, req);

        BOOST_REQUIRE(mock.error == AKU_SUCCESS);
        u32 ix = 0;
        for (auto ts: model_timestamps) {
            for (auto id: col) {
                BOOST_REQUIRE(mock.paramids.at(ix) == id);
                BOOST_REQUIRE(mock.timestamps.at(ix) == ts);
                double expected = ts*0.1;
                double xs = mock.columns[0][ix];
                BOOST_REQUIRE_CLOSE(expected, xs, 10E-10);
                ix++;
            }
        }
        BOOST_REQUIRE(ix != 0);
    };
    test_series_order(10);
    test_series_order(100);
    test_time_order(10);
    test_time_order(100);
}

BOOST_AUTO_TEST_CASE(Test_column_store_group_aggregate_1) {
    test_group_aggregate(100, 1100);
}

BOOST_AUTO_TEST_CASE(Test_column_store_group_aggregate_2) {
    test_group_aggregate(1000, 11000);
}

//! Tests aggregate query in conjunction with group-by clause
void test_aggregate_and_group_by(aku_Timestamp begin, aku_Timestamp end) {
    auto cstore = create_cstore();
    auto session = create_session(cstore);
    std::vector<aku_ParamId> ids = {
        10,11,12,13,14,15,16,17,18,19
    };
    std::vector<double> sums = { 0.0, 0.0 };
    for (auto id: ids) {
        double sum = fill_data_in(cstore, session, id, begin, end);
        if (id % 2 == 0) {
            sums[1] += sum;
        } else {
            sums[0] += sum;
        }
    }
    QueryProcessorMock mock;
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.func = { AggregationFunction::SUM };
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({ids});
    req.group_by.enabled = true;
    req.select.matcher = std::make_shared<PlainSeriesMatcher>(1);
    req.select.matcher->_add("odd", 100);
    req.select.matcher->_add("even", 200);
    req.group_by.transient_map = {
        { 11, 100 },
        { 13, 100 },
        { 15, 100 },
        { 17, 100 },
        { 19, 100 },
        { 10, 200 },
        { 12, 200 },
        { 14, 200 },
        { 16, 200 },
        { 18, 200 },
    };

    execute(cstore, &mock, req);

    std::vector<aku_ParamId> gids = { 100, 200 };

    BOOST_REQUIRE_EQUAL(mock.samples.size(), gids.size());
    for (auto i = 0u; i < mock.samples.size(); i++) {
        BOOST_REQUIRE_EQUAL(mock.samples.at(i).paramid, gids.at(i));
        BOOST_REQUIRE_CLOSE(mock.samples.at(i).payload.float64, sums.at(i), 10E-5);
    }
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregate_group_by_1) {
    test_aggregate_and_group_by(10, 110);
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregate_group_by_2) {
    test_aggregate_and_group_by(100, 1100);
}

BOOST_AUTO_TEST_CASE(Test_column_store_aggregate_group_by_3) {
    test_aggregate_and_group_by(1000, 11000);
}
