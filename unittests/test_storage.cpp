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

void check_timestamps(CursorMock const& mock, std::vector<aku_Timestamp> expected, OrderBy order, std::vector<std::string> names) {
    size_t tsix = 0;
    if (order == OrderBy::SERIES) {
        for (auto s: names) {
            for (auto expts: expected) {
                if (expts != mock.samples.at(tsix++).timestamp) {
                    BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix++).timestamp);
                }
            }
        }
        BOOST_REQUIRE_EQUAL(tsix, mock.samples.size());
    } else {
        for (auto expts: expected) {
            for (auto s: names) {
                if (expts != mock.samples.at(tsix++).timestamp) {
                    BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix++).timestamp);
                }
            }
        }
        BOOST_REQUIRE_EQUAL(tsix, mock.samples.size());
    }
}

static void check_paramids(StorageSession& session,
                           CursorMock const& cursor,
                           OrderBy order,
                           std::vector<std::string> expected_series_names,
                           size_t nelem,
                           bool reverse_dir)
{
    if (order == OrderBy::SERIES) {
        auto elperseries = nelem / expected_series_names.size();
        assert(nelem % expected_series_names.size() == 0);
        size_t iter = 0;
        for (auto expected: expected_series_names) {
            for (size_t i = 0; i < elperseries; i++) {
                const size_t buffer_size = 1024;
                char buffer[buffer_size];
                auto id = cursor.samples.at(iter++).paramid;
                auto len = session.get_series_name(id, buffer, buffer_size);
                if (len < 0) {
                    BOOST_FAIL("Can't extract series name from session");
                }
                std::string actual(buffer, buffer + len);
                if (actual != expected) {
                    BOOST_REQUIRE_EQUAL(actual, expected);
                }
            }
        }
        BOOST_REQUIRE_EQUAL(cursor.samples.size(), iter);
    } else {
        if (reverse_dir) {
            std::reverse(expected_series_names.begin(), expected_series_names.end());
        }
        auto elperseries = nelem / expected_series_names.size();
        assert(nelem % expected_series_names.size() == 0);
        size_t iter = 0;
        for (size_t i = 0; i < elperseries; i++) {
            for (auto expected: expected_series_names) {
                const size_t buffer_size = 1024;
                char buffer[buffer_size];
                auto id = cursor.samples.at(iter++).paramid;
                auto len = session.get_series_name(id, buffer, buffer_size);
                if (len < 0) {
                    BOOST_FAIL("Can't extract series name from session");
                }
                std::string actual(buffer, buffer + len);
                if (actual != expected) {
                    BOOST_REQUIRE_EQUAL(actual, expected);
                }
            }
        }
        BOOST_REQUIRE_EQUAL(cursor.samples.size(), iter);
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
    check_timestamps(cursor, expected, order, series_names);
    check_paramids(*session, cursor, order, series_names, expected_size, begin > end);
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

// Test metadata query

static void test_metadata_query() {
    auto query = "{\"select\": \"names\"}";
    auto storage = create_storage();
    auto session = storage->create_write_session();
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
    for (auto name: series_names) {
        aku_Sample s;
        auto status = session->init_series_id(name.data(), name.data() + name.size(), &s);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        s.timestamp = 111;
        s.payload.type = AKU_PAYLOAD_FLOAT;
        s.payload.float64 = 0.;
        status = session->write(s);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    }
    Caller caller;
    CursorMock cursor;
    session->query(caller, &cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), series_names.size());
    for (auto sample: cursor.samples) {
        const int buffer_size = AKU_LIMITS_MAX_SNAME;
        char buffer[buffer_size];
        auto len = session->get_series_name(sample.paramid, buffer, buffer_size);
        if (len <= 0) {
            BOOST_FAIL("no such id");
        }
        std::string name(buffer, buffer + len);
        auto cnt = std::count(series_names.begin(), series_names.end(), name);
        BOOST_REQUIRE_EQUAL(cnt, 1);
    }
}

BOOST_AUTO_TEST_CASE(Test_storage_metadata_query) {
    test_metadata_query();
}

// Group-by query

static const aku_Timestamp gb_begin = 100;
static const aku_Timestamp gb_end   = 200;

static std::string make_group_by_query(std::string tag, OrderBy order) {
    std::stringstream str;
    str << "{ \"metric\": \"test\",";
    str << "  \"range\": { \"from\": " << gb_begin << ", \"to\": " << gb_end << "},";
    str << "  \"order-by\": " << (order == OrderBy::SERIES ? "\"series\"": "\"time\"") << ",";
    str << "  \"group-by\": {\"tag\": " << "\"" << tag << "\"}";
    str << "}";
    return str.str();
}

static void test_storage_group_by_query(OrderBy order) {
    std::vector<std::string> series_names = {
        "test key=0 group=0",
        "test key=1 group=0",
        "test key=2 group=0",
        "test key=3 group=1",
        "test key=4 group=1",
        "test key=5 group=1",
        "test key=6 group=1",
        "test key=7 group=1",
        "test key=8 group=0",
        "test key=9 group=0",
    };
    // Series names after group-by
    std::vector<std::string> expected_series_names = {
        "test group=0",
        "test group=0",
        "test group=0",
        "test group=0",
        "test group=0",
        "test group=1",
        "test group=1",
        "test group=1",
        "test group=1",
        "test group=1",
    };
    std::vector<std::string> unique_expected_series_names = {
        "test group=0",
        "test group=1",
    };
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, gb_begin, gb_end, series_names);
    Caller caller;
    CursorMock cursor;
    auto query = make_group_by_query("group", order);
    session->query(caller, &cursor, query.c_str());
    BOOST_REQUIRE(cursor.done);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    size_t expected_size;
    expected_size = (gb_end - gb_begin)*series_names.size();
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_size);
    std::vector<aku_Timestamp> expected_timestamps;
    for (aku_Timestamp ts = gb_begin; ts < gb_end; ts++) {
        for (int i = 0; i < 5; i++) {
            expected_timestamps.push_back(ts);
        }
    }
    check_timestamps(cursor, expected_timestamps, order, unique_expected_series_names);
    check_paramids(*session, cursor, order, expected_series_names, (gb_end - gb_begin)*series_names.size(), false);
}

BOOST_AUTO_TEST_CASE(Test_storage_groupby_query_0) {
    test_storage_group_by_query(OrderBy::SERIES);
}

BOOST_AUTO_TEST_CASE(Test_storage_groupby_query_1) {
    test_storage_group_by_query(OrderBy::TIME);
}

// Test where clause

static std::string make_scan_query_with_where(aku_Timestamp begin, aku_Timestamp end, std::vector<int> keys) {
    std::stringstream str;
    str << "{ \"range\": { \"from\": " << begin << ", \"to\": " << end << "},";
    str << "  \"metric\": \"test\",";
    str << "  \"order-by\": \"series\",";
    str << "  \"where\": { \"key\": [";
    bool first = true;
    for (auto key: keys) {
        if (first) {
            first = false;
            str << key;
        } else {
            str << ", " << key;
        }
    }
    str << "]}}";
    return str.str();

}

void test_storage_where_clause(aku_Timestamp begin, aku_Timestamp end, int nseries) {
    std::vector<std::string> series_names;
    for (int i = 0; i < nseries; i++) {
        series_names.push_back("test key=" + std::to_string(i));
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, std::min(begin, end), std::max(begin, end), series_names);
    auto check_case = [&](std::vector<int> ids2read) {
        Caller caller;
        CursorMock cursor;
        auto query = make_scan_query_with_where(begin, end, ids2read);
        std::vector<std::string> expected_series;
        for(auto id: ids2read) {
            expected_series.push_back(series_names[static_cast<size_t>(id)]);
        }
        session->query(caller, &cursor, query.c_str());
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
        size_t expected_size = (end - begin)*expected_series.size();
        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_size);
        std::vector<aku_Timestamp> expected;
        for (aku_Timestamp ts = begin; ts < end; ts++) {
            expected.push_back(ts);
        }
        check_timestamps(cursor, expected, OrderBy::SERIES, expected_series);
        check_paramids(*session, cursor, OrderBy::SERIES, expected_series, expected_size, true);
    };

    std::vector<int> first = { 0 };
    check_case(first);

    std::vector<int> last = { nseries - 1 };
    check_case(last);

    std::vector<int> all; for (int i = 0; i < nseries; i++) { all.push_back(i); };
    check_case(all);

    std::vector<int> even; std::copy_if(all.begin(), all.end(), std::back_inserter(even), [](int i) {return i % 2 == 0;});
    check_case(even);

    std::vector<int> odd; std::copy_if(all.begin(), all.end(), std::back_inserter(odd), [](int i) {return i % 2 != 0;});
    check_case(odd);
}

BOOST_AUTO_TEST_CASE(Test_storage_where_clause) {
    std::vector<std::tuple<aku_Timestamp, aku_Timestamp, int>> cases = {
        std::make_tuple(100, 200, 10),
    };
    for (auto tup: cases) {
        aku_Timestamp begin, end;
        int nseries;
        std::tie(begin, end, nseries) = tup;
        test_storage_where_clause(begin, end, nseries);
    }
}

// Test reopen

