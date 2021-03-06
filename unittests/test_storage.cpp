#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <vector>

#include "queryprocessor_framework.h"
#include "metadatastorage.h"
#include "storage2.h"
#include "query_processing/queryparser.h"

#include "akumuli.h"
#include "log_iface.h"
#include "status_util.h"

// To initialize apr and sqlite properly
#include <apr.h>
#include <sqlite3.h>

using namespace Akumuli;
using namespace StorageEngine;
using namespace QP;

void test_logger(aku_LogLevel tag, const char* msg) {
    AKU_UNUSED(tag);
    AKU_UNUSED(msg);
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
    return meta;
}

std::shared_ptr<ColumnStore> create_cstore() {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore));
    return cstore;
}

std::shared_ptr<Storage> create_storage(bool start_worker=false) {
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto store = std::make_shared<Storage>(meta, bstore, cstore, start_worker);
    return store;
}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_volumes_config) {

    MetadataStorage db(":memory:");
    std::vector<MetadataStorage::VolumeDesc> volumes = {
        { 0, "first", 1, 2, 3, 4 },
        { 1, "second", 5, 6, 7, 8 },
        { 2, "third", 9, 10, 11, 12 },
    };
    db.init_volumes(volumes);
    auto actual = db.get_volumes();
    for (size_t i = 0; i < 3; i++) {
        BOOST_REQUIRE_EQUAL(volumes.at(i).id, actual.at(i).id);
        BOOST_REQUIRE_EQUAL(volumes.at(i).path, actual.at(i).path);
        BOOST_REQUIRE_EQUAL(volumes.at(i).capacity, actual.at(i).capacity);
        BOOST_REQUIRE_EQUAL(volumes.at(i).generation, actual.at(i).generation);
        BOOST_REQUIRE_EQUAL(volumes.at(i).nblocks, actual.at(i).nblocks);
        BOOST_REQUIRE_EQUAL(volumes.at(i).version, actual.at(i).version);
    }
}

BOOST_AUTO_TEST_CASE(Test_metadata_storage_numeric_config) {

    MetadataStorage db(":memory:");
    const char* creation_datetime = "2015-02-03 00:00:00";  // Formatting not required
    const char* bstore_type = "FixedSizeFileStorage";
    const char* db_name = "db_test";
    db.init_config(db_name, creation_datetime, bstore_type);
    std::string actual_dt;
    bool success = db.get_config_param("creation_datetime", &actual_dt);
    BOOST_REQUIRE(success);
    BOOST_REQUIRE_EQUAL(creation_datetime, actual_dt);
    std::string actual_bstore_type;
    success = db.get_config_param("blockstore_type", &actual_bstore_type);
    BOOST_REQUIRE(success);
    BOOST_REQUIRE_EQUAL(bstore_type, actual_bstore_type);
    std::string actual_db_name;
    success = db.get_config_param("db_name", &actual_db_name);
    BOOST_REQUIRE(success);
    BOOST_REQUIRE_EQUAL(db_name, actual_db_name);
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

void fill_data(std::shared_ptr<StorageSession> session,
               std::vector<std::string> const& names,
               std::vector<aku_Timestamp> const& tss,
               std::vector<double> const& xss)
{
    assert(tss.size() == xss.size());
    for (u32 ix = 0; ix < tss.size(); ix++) {
        auto ts = tss.at(ix);
        auto xs = xss.at(ix);
        for (auto it: names) {
            aku_Sample sample;
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.timestamp = ts;
            sample.payload.float64 = xs;
            if (xs > 0 && xs < 1.0E-200) {
                std::cout << "bug" << std::endl;
            }
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
    std::vector<std::vector<double>> tuples;
    aku_Status error;
    std::string error_msg;

    CursorMock() {
        done = false;
        error = AKU_SUCCESS;
    }

    virtual bool put(const aku_Sample &val) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        samples.push_back(val);
        if (val.payload.type & aku_PData::TUPLE_BIT) {
            std::vector<double> outtup;
            u64 bits;
            memcpy(&bits, &val.payload.float64, sizeof(u64));
            double const* tuple = reinterpret_cast<double const*>(val.payload.data);
            int nelements = bits >> 58;
            int tup_ix = 0;
            for (int ix = 0; ix < nelements; ix++) {
                if (bits & (1 << ix)) {
                    outtup.push_back(tuple[tup_ix++]);
                } else {
                    // Empty tuple value. Use NaN to represent the hole.
                    outtup.push_back(NAN);
                }
            }
            tuples.push_back(std::move(outtup));
        }
        return true;
    }

    virtual void complete() override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        done = true;
    }

    virtual void set_error( aku_Status error_code) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        done = true;
        error = error_code;
    }

    virtual void set_error(aku_Status error_code, const char* error_message) override {
        if (done) {
            BOOST_FAIL("Cursor invariant broken");
        }
        done = true;
        error = error_code;
        error_msg = error_message;
    }
};

std::string make_scan_query(aku_Timestamp begin, aku_Timestamp end, OrderBy order) {
    std::stringstream str;
    str << "{ \"select\": \"test\", \"range\": { \"from\": " << begin << ", \"to\": " << end << "},";
    str << "  \"order-by\": " << (order == OrderBy::SERIES ? "\"series\"" : "\"time\"");
    str << "}";
    return str.str();
}

void check_timestamps(CursorMock const& mock, std::vector<aku_Timestamp> expected, OrderBy order, std::vector<std::string> names) {
    size_t tsix = 0;
    if (order == OrderBy::SERIES) {
        for (auto s: names) {
            for (auto expts: expected) {
                if (expts != mock.samples.at(tsix).timestamp) {
                    BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix).timestamp);
                }
                tsix++;
            }
        }
        BOOST_REQUIRE_EQUAL(tsix, mock.samples.size());
    } else {
        for (auto expts: expected) {
            for (auto s: names) {
                if (expts != mock.samples.at(tsix).timestamp) {
                    BOOST_REQUIRE_EQUAL(expts, mock.samples.at(tsix).timestamp);
                }
                tsix++;
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
    CursorMock cursor;
    auto query = make_scan_query(begin, end, order);
    session->query(&cursor, query.c_str());
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
    auto query = "{\"select\": \"meta:names\"}";
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
    CursorMock cursor;
    session->query(&cursor, query);
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

// Test suggest

static void test_suggest_metric_name() {
    auto query = "{\"select\": \"metric-names\", \"starts-with\": \"test\" }";
    auto storage = create_storage();
    auto session = storage->create_write_session();
    std::set<std::string> expected_metric_names = {
        "test.aaa",
        "test.bbb",
        "test.ccc",
        "test.ddd",
        "test.eee",
    };
    std::vector<std::string> series_names = {
        "test.aaa key=0",
        "test.aaa key=1",
        "test.bbb key=2",
        "test.bbb key=3",
        "test.ccc key=4",
        "test.ccc key=5",
        "test.ddd key=6",
        "test.ddd key=7",
        "test.eee key=8",
        "test.eee key=9",
        "fff.test key=0",
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
    CursorMock cursor;
    session->suggest(&cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_metric_names.size());
    for (auto sample: cursor.samples) {
        const int buffer_size = AKU_LIMITS_MAX_SNAME;
        char buffer[buffer_size];
        auto len = session->get_series_name(sample.paramid, buffer, buffer_size);
        if (len <= 0) {
            BOOST_FAIL("no such id");
        }
        std::string name(buffer, buffer + len);
        auto cnt = expected_metric_names.count(name);
        BOOST_REQUIRE_EQUAL(cnt, 1);
        // Ensure no duplicates
        expected_metric_names.erase(expected_metric_names.find(name));
    }
}

BOOST_AUTO_TEST_CASE(Test_storage_suggest_query_1) {
    test_suggest_metric_name();
}

static void test_suggest_tag_name() {
    auto query = "{\"select\": \"tag-names\", \"metric\": \"test\", \"starts-with\": \"ba\" }";
    auto storage = create_storage();
    auto session = storage->create_write_session();
    std::set<std::string> expected_tag_names = {
        "bar",
        "baar",
        "babr",
        "badr",
        "baer",
    };
    std::vector<std::string> series_names = {
        "test foo=0 bar=0",
        "test foo=1 bar=1",
        "test foo=0 bar=0 baar=0",
        "test foo=1 bar=1 babr=1",
        "tost foo=0 bar=0 bacr=0",
        "test foo=1 bar=1 badr=1",
        "test foo=0 bar=0 baer=0",
        "test foo=1 bar=1 baer=0",
        "test foo=1 bar=1",
        "test foo=0 bar=0",
        "test foo=1 bar=1",
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
    CursorMock cursor;
    session->suggest(&cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_tag_names.size());
    for (auto sample: cursor.samples) {
        const int buffer_size = AKU_LIMITS_MAX_SNAME;
        char buffer[buffer_size];
        auto len = session->get_series_name(sample.paramid, buffer, buffer_size);
        if (len <= 0) {
            BOOST_FAIL("no such id");
        }
        std::string name(buffer, buffer + len);
        auto cnt = expected_tag_names.count(name);
        BOOST_REQUIRE_EQUAL(cnt, 1);
        // Ensure no duplicates
        expected_tag_names.erase(expected_tag_names.find(name));
    }
}

BOOST_AUTO_TEST_CASE(Test_storage_suggest_query_2) {
    test_suggest_tag_name();
}

static void test_suggest_tag_values() {
    auto query = "{\"select\": \"tag-values\", \"metric\": \"test\", \"tag\":\"foo\", \"starts-with\": \"ba\" }";
    auto storage = create_storage();
    auto session = storage->create_write_session();
    std::set<std::string> expected_tag_values = {
        "bar",
        "baar",
        "bacr",
        "baer",
        "ba",
    };
    std::vector<std::string> series_names = {
        "test key=00000 foo=bar",
        "test key=00000 foo=buz",
        "test key=00000 foo=baar",
        "tost key=00000 foo=babr",
        "test key=00000 foo=bacr",
        "test key=00000 fuz=badr",
        "test key=00000 foo=baer",
        "test key=00000 foo=bin",
        "test key=00000 foo=foo",
        "test key=00000 foo=ba",
        "test key=00001 foo=bar",
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
    CursorMock cursor;
    session->suggest(&cursor, query);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected_tag_values.size());
    for (auto sample: cursor.samples) {
        const int buffer_size = AKU_LIMITS_MAX_SNAME;
        char buffer[buffer_size];
        auto len = session->get_series_name(sample.paramid, buffer, buffer_size);
        if (len <= 0) {
            BOOST_FAIL("no such id");
        }
        std::string name(buffer, buffer + len);
        auto cnt = expected_tag_values.count(name);
        BOOST_REQUIRE_EQUAL(cnt, 1);
        // Ensure no duplicates
        expected_tag_values.erase(expected_tag_values.find(name));
    }
}

BOOST_AUTO_TEST_CASE(Test_storage_suggest_query_3) {
    test_suggest_tag_values();
}

// Group-by query

static const aku_Timestamp gb_begin = 100;
static const aku_Timestamp gb_end   = 200;

static std::string make_group_by_query(std::string tag, OrderBy order) {
    std::stringstream str;
    str << "{ \"select\": \"test\",";
    str << "  \"range\": { \"from\": " << gb_begin << ", \"to\": " << gb_end << "},";
    str << "  \"order-by\": " << (order == OrderBy::SERIES ? "\"series\"": "\"time\"") << ",";
    str << "  \"group-by\": [\"" << tag << "\"]";
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
    CursorMock cursor;
    auto query = make_group_by_query("group", order);
    session->query(&cursor, query.c_str());
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

// Test aggregate query

BOOST_AUTO_TEST_CASE(Test_storage_aggregate_query) {
    std::vector<std::string> series_names_1 = {
        "cpu.user key=0 group=1",
        "cpu.user key=1 group=1",
        "cpu.user key=2 group=1",
        "cpu.user key=3 group=1",
        "cpu.syst key=0 group=1",
        "cpu.syst key=1 group=1",
        "cpu.syst key=2 group=1",
        "cpu.syst key=3 group=1",
    };
    std::vector<std::string> series_names_0 = {
        "cpu.user key=4 group=0",
        "cpu.user key=5 group=0",
        "cpu.user key=6 group=0",
        "cpu.user key=7 group=0",
        "cpu.syst key=4 group=0",
        "cpu.syst key=5 group=0",
        "cpu.syst key=6 group=0",
        "cpu.syst key=7 group=0",
    };
    std::vector<double> xss_1, xss_0;
    std::vector<aku_Timestamp> tss_all;
    const aku_Timestamp BASE_TS = 100000, STEP_TS = 1000;
    const double BASE_X0 = 1.0E3, STEP_X0 = 10.0;
    const double BASE_X1 = -10., STEP_X1 = -10.0;
    for (int i = 0; i < 10000; i++) {
        tss_all.push_back(BASE_TS + i*STEP_TS);
        xss_0.push_back(BASE_X0 + i*STEP_X0);
        xss_1.push_back(BASE_X1 + i*STEP_X1);
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, series_names_0, tss_all, xss_0);
    fill_data(session, series_names_1, tss_all, xss_1);

    {
        // Construct aggregate query
        const char* query = R"==(
                {
                    "aggregate": {
                        "cpu.user": "min",
                        "cpu.syst": "max"
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), series_names_0.size() + series_names_1.size());

        std::vector<std::pair<std::string, double>> expected = {
            std::make_pair("cpu.user:min group=0 key=4", 1000),
            std::make_pair("cpu.user:min group=0 key=5", 1000),
            std::make_pair("cpu.user:min group=0 key=6", 1000),
            std::make_pair("cpu.user:min group=0 key=7", 1000),
            std::make_pair("cpu.user:min group=1 key=0", -100000),
            std::make_pair("cpu.user:min group=1 key=1", -100000),
            std::make_pair("cpu.user:min group=1 key=2", -100000),
            std::make_pair("cpu.user:min group=1 key=3", -100000),
            std::make_pair("cpu.syst:max group=0 key=4", 100990),
            std::make_pair("cpu.syst:max group=0 key=5", 100990),
            std::make_pair("cpu.syst:max group=0 key=6", 100990),
            std::make_pair("cpu.syst:max group=0 key=7", 100990),
            std::make_pair("cpu.syst:max group=1 key=0", -10),
            std::make_pair("cpu.syst:max group=1 key=1", -10),
            std::make_pair("cpu.syst:max group=1 key=2", -10),
            std::make_pair("cpu.syst:max group=1 key=3", -10),
        };

        int i = 0;
        for (const auto& sample: cursor.samples) {
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            BOOST_REQUIRE_EQUAL(expected.at(i).first, sname);
            BOOST_REQUIRE_EQUAL(expected.at(i).second, sample.payload.float64);
            i++;
        }
    }

    {
        // Construct aggregate - group-by query
        const char* query = R"==(
                {
                    "aggregate": {
                        "cpu.user": "min",
                        "cpu.syst": "max"
                    },
                    "group-by": [ "group" ]
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), 4);

        std::vector<std::pair<std::string, double>> expected = {
            std::make_pair("cpu.user:min group=0", 1000),
            std::make_pair("cpu.user:min group=1", -100000),
            std::make_pair("cpu.syst:max group=0", 100990),
            std::make_pair("cpu.syst:max group=1", -10),
        };

        int i = 0;
        for (const auto& sample: cursor.samples) {
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            BOOST_REQUIRE_EQUAL(expected.at(i).first, sname);
            BOOST_REQUIRE_EQUAL(expected.at(i).second, sample.payload.float64);
            i++;
        }
    }
}

// Test group aggregate query

BOOST_AUTO_TEST_CASE(Test_storage_group_aggregate_query_0) {
    std::vector<std::string> series_names = {
        "cpu.syst key=0 group=0",
        "cpu.syst key=1 group=0",
        "cpu.syst key=2 group=1",
        "cpu.syst key=3 group=1",
        "cpu.user key=0 group=0",
        "cpu.user key=1 group=0",
        "cpu.user key=2 group=1",
        "cpu.user key=3 group=1",
    };
    std::vector<double> xss;
    std::vector<aku_Timestamp> tss;
    const aku_Timestamp BASE_TS = 100000, STEP_TS = 1000;
    const double BASE_X = 1.0E3, STEP_X = 10.0;
    for (int i = 0; i < 10000; i++) {
        tss.push_back(BASE_TS + i*STEP_TS);
        xss.push_back(BASE_X + i*STEP_X);
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, series_names, tss, xss);

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": "cpu.user",
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double>> expected = {
            std::make_tuple("cpu.user:min group=0 key=0", 100000,  1000),
            std::make_tuple("cpu.user:min group=0 key=1", 100000,  1000),
            std::make_tuple("cpu.user:min group=1 key=2", 100000,  1000),
            std::make_tuple("cpu.user:min group=1 key=3", 100000,  1000),
            std::make_tuple("cpu.user:min group=0 key=0", 4100000, 41000),
            std::make_tuple("cpu.user:min group=0 key=1", 4100000, 41000),
            std::make_tuple("cpu.user:min group=1 key=2", 4100000, 41000),
            std::make_tuple("cpu.user:min group=1 key=3", 4100000, 41000),
            std::make_tuple("cpu.user:min group=0 key=0", 8100000, 81000),
            std::make_tuple("cpu.user:min group=0 key=1", 8100000, 81000),
            std::make_tuple("cpu.user:min group=1 key=2", 8100000, 81000),
            std::make_tuple("cpu.user:min group=1 key=3", 8100000, 81000),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 1);
            BOOST_REQUIRE((bits & 1) == 1);
            double pmin = cursor.tuples[i][0];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            i++;
        }
    }

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "group-by-tag": [ "key" ],
                    "order-by": "series",
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double>> expected = {
            std::make_tuple("cpu.user:min group=0", 100000, 1000),
            std::make_tuple("cpu.user:min group=0", 4100000, 41000),
            std::make_tuple("cpu.user:min group=0", 8100000, 81000),
            std::make_tuple("cpu.user:min group=1", 100000, 1000),
            std::make_tuple("cpu.user:min group=1", 4100000, 41000),
            std::make_tuple("cpu.user:min group=1", 8100000, 81000),
            std::make_tuple("cpu.syst:min group=0", 100000, 1000),
            std::make_tuple("cpu.syst:min group=0", 4100000, 41000),
            std::make_tuple("cpu.syst:min group=0", 8100000, 81000),
            std::make_tuple("cpu.syst:min group=1", 100000, 1000),
            std::make_tuple("cpu.syst:min group=1", 4100000, 41000),
            std::make_tuple("cpu.syst:min group=1", 8100000, 81000),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 1);
            BOOST_REQUIRE((bits & 1) == 1);
            double pmin = cursor.tuples[i][0];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            i++;
        }
    }

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": "cpu.user",
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "pivot-by-tag": [ "group" ],
                    "order-by": "time",
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double>> expected = {
            std::make_tuple("cpu.user:min group=0", 100000, 1000),
            std::make_tuple("cpu.user:min group=1", 100000, 1000),
            std::make_tuple("cpu.user:min group=0", 4100000, 41000),
            std::make_tuple("cpu.user:min group=1", 4100000, 41000),
            std::make_tuple("cpu.user:min group=0", 8100000, 81000),
            std::make_tuple("cpu.user:min group=1", 8100000, 81000),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 1);
            BOOST_REQUIRE((bits & 1) == 1);
            double pmin = cursor.tuples[i][0];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            i++;
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_storage_group_aggregate_query_1) {
    std::vector<std::string> series_names = {
        "cpu.syst key=0 group=0",
        "cpu.syst key=1 group=0",
        "cpu.syst key=2 group=1",
        "cpu.syst key=3 group=1",
        "cpu.user key=0 group=0",
        "cpu.user key=1 group=0",
        "cpu.user key=2 group=1",
        "cpu.user key=3 group=1",
    };
    std::vector<double> xss;
    std::vector<aku_Timestamp> tss;
    const aku_Timestamp BASE_TS = 100000, STEP_TS = 1000;
    const double BASE_X = 1.0E3, STEP_X = 10.0;
    for (int i = 0; i < 10000; i++) {
        tss.push_back(BASE_TS + i*STEP_TS);
        xss.push_back(BASE_X + i*STEP_X);
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, series_names, tss, xss);

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : ["min", "max"]
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double, double>> expected = {
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=0", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=1", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=2", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=3", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=0", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=1", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=2", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=3", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=2", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=3", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=2", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=3", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0 key=1", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=2", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1 key=3", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0 key=1", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=2", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1 key=3", 8100000, 81000, 100990),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            BOOST_REQUIRE((bits & 3) == 3);
            double pmin = cursor.tuples[i][0], pmax = cursor.tuples[i][1];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), pmax);
            i++;
        }
    }

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : ["min", "max"]
                    },
                    "group-by-tag": ["key"],
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double, double>> expected = {
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 8100000, 81000, 100990),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            BOOST_REQUIRE((bits & 3) == 3);
            double pmin = cursor.tuples[i][0], pmax = cursor.tuples[i][1];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), pmax);
            i++;
        }
    }

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : ["min", "max"]
                    },
                    "group-by-tag": ["key"],
                    "range": {
                        "from"  : 10100000,
                        "to"    : 100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double, double>> expected = {
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 10100000, 61010, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 10100000, 61010, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 10100000, 61010, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 10100000, 61010, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 6100000, 21010, 61000),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 6100000, 21010, 61000),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 6100000, 21010, 61000),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 6100000, 21010, 61000),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 2100000, 1010, 21000),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 2100000, 1010, 21000),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 2100000, 1010, 21000),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 2100000, 1010, 21000),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            BOOST_REQUIRE((bits & 3) == 3);
            double pmin = cursor.tuples[i][0], pmax = cursor.tuples[i][1];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), pmax);
            i++;
        }
    }

    {
        // Construct group-aggregate query
        const char* query = R"==(
                {
                    "group-aggregate": {
                        "metric"     : ["cpu.user", "cpu.syst"],
                        "step"       : 4000000,
                        "func"       : ["min", "max"]},
                    "group-by-tag"   : ["key"],
                    "order-by"       : "series",
                    "range"          : {
                        "from"       : 100000,
                        "to"         : 10100000}
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double, double>> expected = {
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 100000, 1000, 40990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.user:min|cpu.user:max group=1", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=0", 8100000, 81000, 100990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 100000, 1000, 40990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 4100000, 41000, 80990),
            std::make_tuple("cpu.syst:min|cpu.syst:max group=1", 8100000, 81000, 100990),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            BOOST_REQUIRE((bits & 3) == 3);
            double pmin = cursor.tuples[i][0], pmax = cursor.tuples[i][1];
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), pmin);
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), pmax);
            i++;
        }
    }

}

// Test where clause

static std::string make_scan_query_with_where(aku_Timestamp begin, aku_Timestamp end, std::vector<int> keys) {
    std::stringstream str;
    str << "{ \"range\": { \"from\": " << begin << ", \"to\": " << end << "},";
    str << "  \"select\": \"test\",";
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
    str << "], \"zzz\": 0 }}";
    return str.str();

}

void test_storage_where_clause(aku_Timestamp begin, aku_Timestamp end, int nseries) {
    std::vector<std::string> series_names;
    std::vector<std::string> all_series_names;
    for (int i = 0; i < nseries; i++) {
        series_names.push_back("test key=" + std::to_string(i) + " zzz=0");
        all_series_names.push_back("test key=" + std::to_string(i) + " zzz=0");
        all_series_names.push_back("test key=" + std::to_string(i) + " zzz=1");
    }
    // We will fill all series but will read only zzz=0 series to check multi-tag queries
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, std::min(begin, end), std::max(begin, end), all_series_names);
    auto check_case = [&](std::vector<int> ids2read) {
        CursorMock cursor;
        auto query = make_scan_query_with_where(begin, end, ids2read);
        std::vector<std::string> expected_series;
        for(auto id: ids2read) {
            expected_series.push_back(series_names[static_cast<size_t>(id)]);
        }
        session->query(&cursor, query.c_str());
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

static void test_storage_where_clause2(aku_Timestamp begin, aku_Timestamp end) {
    int nseries = 100;
    std::vector<std::string> series_names;
    std::vector<std::string> expected_series = {
        "test key=10 zzz=0",
        "test key=22 zzz=0",
        "test key=42 zzz=0",
        "test key=66 zzz=0"
    };
    for (int i = 0; i < nseries; i++) {
        series_names.push_back("test key=" + std::to_string(i) + " zzz=0");
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, std::min(begin, end), std::max(begin, end), series_names);

    std::stringstream query;
    query << "{";
    query << "   \"select\": \"test\",\n";
    query << "   \"where\": [\n";
    query << "       { \"key\": 10, \"zzz\": 0 },\n";
    query << "       { \"key\": 14             },\n";  // should be missing
    query << "       { \"key\": 22, \"zzz\": 0 },\n";
    query << "       { \"key\": 42, \"zzz\": 0 },\n";
    query << "       { \"key\": 66, \"zzz\": 0 }\n";
    query << "   ],\n";
    query << "   \"order-by\": \"series\",\n";
    query << "   \"range\": { \"from\": " << begin << ", \"to\": " << end << "}\n";
    query << "}";

    CursorMock cursor;
    session->query(&cursor, query.str().c_str());
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
}

BOOST_AUTO_TEST_CASE(Test_storage_where_form2) {
    aku_Timestamp begin = 100, end = 200;
    test_storage_where_clause2(begin, end);
}

// Test SeriesRetreiver

void test_retreiver() {
    std::vector<u64> ids;
    std::vector<std::string> test_data = {
        "aaa foo=1 bar=1 buz=1",
        "aaa foo=1 bar=1 buz=2",
        "aaa foo=1 bar=2 buz=2",
        "aaa foo=2 bar=2 buz=2",
        "aaa foo=2 bar=2 buz=3",
        "bbb foo=2 bar=3 buz=3",
        "bbb foo=3 bar=3 buz=3",
        "bbb foo=3 bar=3 buz=4",
        "bbb foo=3 bar=4 buz=4",
        "bbb foo=4 bar=4 buz=4",
        "bbb foo=4 bar=4 buz=5",
        "bbb foo=4 bar=4 buz=6",
    };
    PlainSeriesMatcher m;
    for (auto s: test_data) {
        char buffer[0x100];
        const char* pkeys_begin;
        const char* pkeys_end;
        aku_Status status = SeriesParser::to_canonical_form(s.data(), s.data() + s.size(),
                                                            buffer, buffer + 0x100,
                                                            &pkeys_begin, &pkeys_end);
        BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
        auto id = m.add(buffer, pkeys_end);
        ids.push_back(id);
    }

    std::vector<u64> act;
    aku_Status status;

    SeriesRetreiver rt1;
    status = rt1.add_tag("foo", "1");
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_ARG);
    std::tie(status, act) = rt1.extract_ids(m);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(ids.begin(), ids.end(), act.begin(), act.end());

    SeriesRetreiver rt2({"bbb"});
    std::tie(status, act) = rt2.extract_ids(m);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(ids.begin() + 5, ids.end(), act.begin(), act.end());

    SeriesRetreiver rt3({"bbb"});
    status = rt3.add_tag("foo", "3");
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = rt3.add_tag("buz", "4");
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = rt3.add_tag("buz", "4");
    BOOST_REQUIRE_EQUAL(status, AKU_EBAD_ARG);
    std::tie(status, act) = rt3.extract_ids(m);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(ids.begin() + 7, ids.begin() + 9, act.begin(), act.end());

    SeriesRetreiver rt4({"bbb"});
    status = rt4.add_tag("foo", "4");
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = rt4.add_tags("buz", {"4", "5"});
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    std::tie(status, act) = rt4.extract_ids(m);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(ids.begin() + 9, ids.begin() + 11, act.begin(), act.end());
}


BOOST_AUTO_TEST_CASE(Test_series_retreiver_1) {
    test_retreiver();
}

BOOST_AUTO_TEST_CASE(Test_series_add_1) {
    const char* sname = "hello|world tag=1";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto session = store->create_write_session();

    aku_ParamId ids[10];
    auto nids = session->get_series_ids(sname, end, ids, 10);
    BOOST_REQUIRE_EQUAL(nids, 2);

    char buf[100];
    auto buflen = session->get_series_name(ids[0], buf, 100);
    std::string name0(buf, buf + buflen);
    BOOST_REQUIRE_EQUAL(name0, "hello tag=1");
    buflen = session->get_series_name(ids[1], buf, 100);
    std::string name1(buf, buf + buflen);
    BOOST_REQUIRE_EQUAL(name1, "world tag=1");
}

// No input
BOOST_AUTO_TEST_CASE(Test_series_add_2) {
    const char* sname = "";
    const char* end = sname;

    auto store = create_storage();
    auto session = store->create_write_session();

    aku_ParamId ids[10];
    auto nids = session->get_series_ids(sname, end, ids, 10);
    BOOST_REQUIRE_EQUAL(-1*nids, AKU_EBAD_DATA);
}

// No tags
BOOST_AUTO_TEST_CASE(Test_series_add_3) {
    const char* sname = "hello|world";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto session = store->create_write_session();

    aku_ParamId ids[10];
    auto nids = session->get_series_ids(sname, end, ids, 10);
    BOOST_REQUIRE_EQUAL(-1*nids, AKU_EBAD_DATA);
}

// ids array is too small
BOOST_AUTO_TEST_CASE(Test_series_add_4) {
    const char* sname = "hello|world tag=val";
    const char* end = sname + strlen(sname);

    auto store = create_storage();
    auto session = store->create_write_session();

    aku_ParamId ids[1];
    auto nids = session->get_series_ids(sname, end, ids, 1);
    BOOST_REQUIRE_EQUAL(-1*nids, AKU_EBAD_ARG);
}

// Series too long
BOOST_AUTO_TEST_CASE(Test_series_add_5) {
    std::stringstream strstr;
    strstr << "metric0";
    for (int i = 1; i < 1000; i++) {
        strstr << "|metric" << i;
    }
    strstr << " tag=value";

    auto sname = strstr.str();

    auto store = create_storage();
    auto session = store->create_write_session();

    aku_ParamId ids[100];
    auto nids = session->get_series_ids(sname.data(), sname.data() + sname.size(), ids, 100);
    BOOST_REQUIRE_EQUAL(-1*nids, AKU_EBAD_DATA);
}

// Test reopen

void test_wal_recovery(int cardinality, aku_Timestamp begin, aku_Timestamp end) {
    BOOST_REQUIRE(cardinality);
    std::vector<std::string> series_names;
    for (int i = 0; i < cardinality; i++) {
        series_names.push_back(
            "test tag=" + std::to_string(i));
    }
    auto meta = create_metadatastorage();
    auto bstore = BlockStoreBuilder::create_memstore();
    auto cstore = create_cstore();
    auto store = std::make_shared<Storage>(meta, bstore, cstore, true);
    aku_FineTuneParams params;
    params.input_log_concurrency = 1;
    params.input_log_path = "./";
    params.input_log_volume_numb = 32;
    params.input_log_volume_size = 1024*1024*24;
    store->initialize_input_log(params);
    auto session = store->create_write_session();

    fill_data(session, begin, end, series_names);
    session.reset();  // This should flush current WAL frame
    store->_kill();

    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> mapping;
    store = std::make_shared<Storage>(meta, bstore, cstore, true);
    store->run_recovery(params, &mapping);
    store->initialize_input_log(params);
    session = store->create_write_session();

    CursorMock cursor;
    auto query = make_scan_query(begin, end, OrderBy::SERIES);
    session->query(&cursor, query.c_str());
    BOOST_REQUIRE(cursor.done);
    BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);
    auto expected_size = (end - begin)*series_names.size();
    std::vector<aku_Timestamp> expected;
    for (aku_Timestamp ts = begin; ts < end; ts++) {
        expected.push_back(ts);
    }
    if (cursor.samples.size() < expected.size()) {
        BOOST_FAIL("Samples lost");
    }
    check_timestamps(cursor, expected, OrderBy::SERIES, series_names);
    check_paramids(*session, cursor, OrderBy::SERIES, series_names, expected_size, false);

    session.reset();
    store->close();
}

BOOST_AUTO_TEST_CASE(Test_wal_recovery_0) {
    test_wal_recovery(100, 1000, 2000);
}

BOOST_AUTO_TEST_CASE(Test_wal_recovery_1) {
    test_wal_recovery(100, 1000, 11000);
}

BOOST_AUTO_TEST_CASE(Test_wal_recovery_2) {
    test_wal_recovery(100, 1000, 101000);
}

/**
 * @brief Test WAL effect on write amplification
 * @param usewal is a flag that controls use of WAL in the test
 * @param total_cardinality is a total number of series
 * @param batch_size is a number of series in the batch
 * @param begin timestamp
 * @param end timestamp
 *
 * This test writes `total_cardinlity` series in batches.
 * When new batch is being written the previous one should
 * be evicted from the working set (the corresponding
 * nbtree instances should be closed) if WAL is enabled. So,
 * resulting write amplification should be high.
 * If WAL is not used the number of written pages should match
 * `total_cardinality`
 */
void test_wal_write_amplification_impact(bool usewal, int total_cardinality, int batch_size, aku_Timestamp begin, aku_Timestamp end) {
    int nbatches = total_cardinality / batch_size;
    int append_cnt = 0;
    auto counter_fn = [&](LogicAddr) {
        append_cnt++;
    };
    auto bstore = BlockStoreBuilder::create_memstore(counter_fn);
    auto meta = create_metadatastorage();
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore));
    auto store = std::make_shared<Storage>(meta, bstore, cstore, true);
    aku_FineTuneParams params;
    params.input_log_concurrency = 1;
    params.input_log_path = usewal ? "./" : nullptr;
    params.input_log_volume_numb = 4;
    params.input_log_volume_size = 10*0x1000;
    store->initialize_input_log(params);

    for (int ixbatch = 0; ixbatch < nbatches; ixbatch++) {
        auto session = store->create_write_session();
        std::vector<std::string> series_names;
        for (int i = 0; i < batch_size; i++) {
            series_names.push_back(
                "test tag=" + std::to_string(ixbatch*batch_size + i));
        }
        fill_data(session, begin, end, series_names);
        session.reset();  // This should flush current WAL frame
    }
    if (usewal) {
        BOOST_REQUIRE(append_cnt != 0);
    } else {
        BOOST_REQUIRE(append_cnt == 0);
    }
    store->close();
    if (usewal) {
        BOOST_REQUIRE(append_cnt > total_cardinality);
    } else {
        BOOST_REQUIRE(append_cnt == total_cardinality);
    }
}

BOOST_AUTO_TEST_CASE(Test_high_cardinality_0) {
    test_wal_write_amplification_impact(true, 10000, 1000, 1000, 1010);
}

BOOST_AUTO_TEST_CASE(Test_high_cardinality_1) {
    test_wal_write_amplification_impact(false, 10000, 1000, 1000, 1010);
}

BOOST_AUTO_TEST_CASE(Test_group_aggregate_join_query_0) {
    std::vector<std::string> series_names1 = {
        "cpu.user key=0 group=0",
        "cpu.user key=1 group=0",
        "cpu.user key=2 group=1",
        "cpu.user key=3 group=1",
    };
    std::vector<std::string> series_names2 = {
        "cpu.syst key=0 group=0",
        "cpu.syst key=1 group=0",
        "cpu.syst key=2 group=1",
        "cpu.syst key=3 group=1",
    };
    std::vector<double> xss;
    std::vector<aku_Timestamp> tss;
    const aku_Timestamp BASE_TS = 100000, STEP_TS = 1000;
    const double BASE_X = 1.0E3, STEP_X = 10.0;
    for (int i = 0; i < 10000; i++) {
        tss.push_back(BASE_TS + i*STEP_TS);
        xss.push_back(BASE_X + i*STEP_X);
    }
    auto storage = create_storage();
    auto session = storage->create_write_session();
    fill_data(session, series_names1, tss, xss);
    std::transform(xss.begin(), xss.end(), xss.begin(), [](double x) { return x*100; });
    fill_data(session, series_names2, tss, xss);

    {
        // Simple group-aggregate-join query with two columns
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        std::vector<std::tuple<std::string, aku_Timestamp, double, double>> expected = {
            std::make_tuple("cpu.user|cpu.syst group=0 key=0",  100000,  1000,  100000),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1",  100000,  1000,  100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2",  100000,  1000,  100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3",  100000,  1000,  100000),
            std::make_tuple("cpu.user|cpu.syst group=0 key=0", 4100000, 41000, 4100000),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1", 4100000, 41000, 4100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2", 4100000, 41000, 4100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3", 4100000, 41000, 4100000),
            std::make_tuple("cpu.user|cpu.syst group=0 key=0", 8100000, 81000, 8100000),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1", 8100000, 81000, 8100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2", 8100000, 81000, 8100000),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3", 8100000, 81000, 8100000),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            BOOST_REQUIRE((bits & 1) == 1);
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), cursor.tuples[i][0]);
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), cursor.tuples[i][1]);
            i++;
        }
    }

    {
        // Group-aggregate-join query with filter statement
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "filter": {
                        "cpu.user": { "gt": 40000, "lt": 80000 }
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        /* Only one column is filtered so we should see all data-points but some of them
         * should have only second element set
         */
        std::vector<std::tuple<std::string, aku_Timestamp, double, double, bool>> expected = {
            std::make_tuple("cpu.user|cpu.syst group=0 key=0",  100000,  1000,  100000, false),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1",  100000,  1000,  100000, false),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2",  100000,  1000,  100000, false),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3",  100000,  1000,  100000, false),
            std::make_tuple("cpu.user|cpu.syst group=0 key=0", 4100000, 41000, 4100000, true),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1", 4100000, 41000, 4100000, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2", 4100000, 41000, 4100000, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3", 4100000, 41000, 4100000, true),
            std::make_tuple("cpu.user|cpu.syst group=0 key=0", 8100000, 81000, 8100000, false),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1", 8100000, 81000, 8100000, false),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2", 8100000, 81000, 8100000, false),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3", 8100000, 81000, 8100000, false),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            bool col1 = std::get<4>(expected.at(i));
            BOOST_REQUIRE((bits & 1) == col1);
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            if (col1) {
                BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), cursor.tuples[i][0]);
            }
            BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), cursor.tuples[i][1]);
            i++;
        }
    }

    {
        // Group-aggregate-join query with filter on two columns
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "filter": {
                        "cpu.user": { "gt": 3000,  "lt": 80000   },
                        "cpu.syst": { "gt": 99999, "lt": 8100000 }
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_SUCCESS);

        /* Here we should have datapoints that match both filters of one filter. Datapoints that doesn't
         * match both filters shouldn't be here
         */
        std::vector<std::tuple<std::string, aku_Timestamp, double, double, bool, bool>> expected = {
            std::make_tuple("cpu.user|cpu.syst group=0 key=0",  100000,  1000,  100000, false, true),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1",  100000,  1000,  100000, false, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2",  100000,  1000,  100000, false, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3",  100000,  1000,  100000, false, true),
            std::make_tuple("cpu.user|cpu.syst group=0 key=0", 4100000, 41000, 4100000, true, true),
            std::make_tuple("cpu.user|cpu.syst group=0 key=1", 4100000, 41000, 4100000, true, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=2", 4100000, 41000, 4100000, true, true),
            std::make_tuple("cpu.user|cpu.syst group=1 key=3", 4100000, 41000, 4100000, true, true),
        };

        BOOST_REQUIRE_EQUAL(cursor.samples.size(), expected.size());
        BOOST_REQUIRE_EQUAL(cursor.tuples.size(), expected.size());

        int i = 0;
        for (const auto& sample: cursor.samples) {
            BOOST_REQUIRE((sample.payload.type & aku_PData::TUPLE_BIT) != 0);
            char buffer[100];
            int len = session->get_series_name(sample.paramid, buffer, 100);
            std::string sname(buffer, buffer + len);
            u64 bits;
            memcpy(&bits, &sample.payload.float64, sizeof(u64));
            BOOST_REQUIRE((bits >> 58) == 2);
            bool col1 = std::get<4>(expected.at(i));
            bool col2 = std::get<5>(expected.at(i));
            BOOST_REQUIRE(static_cast<bool>(bits & 1) == col1);
            BOOST_REQUIRE(static_cast<bool>(bits & 2) == col2);
            BOOST_REQUIRE_EQUAL(std::get<0>(expected.at(i)), sname);
            BOOST_REQUIRE_EQUAL(std::get<1>(expected.at(i)), sample.timestamp);
            if (col1) {
                BOOST_REQUIRE_EQUAL(std::get<2>(expected.at(i)), cursor.tuples[i][0]);
            }
            if (col2) {
                BOOST_REQUIRE_EQUAL(std::get<3>(expected.at(i)), cursor.tuples[i][1]);
            }
            i++;
        }
    }

    {
        // Group-aggregate-join query with two aggregation functions (not supported)
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : [ "min", "max" ]
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_EQUERY_PARSING_ERROR);
    }

    {
        // Group-aggregate-join query with single column
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_EQUERY_PARSING_ERROR);
    }

    {
        // Group-aggregate-join query with two columns and pivot-by-tag
        const char* query = R"==(
                {
                    "group-aggregate-join": {
                        "metric": ["cpu.user", "cpu.syst"],
                        "step"  : 4000000,
                        "func"  : "min"
                    },
                    "pivot-by-tag": [ "group" ],
                    "range": {
                        "from"  : 100000,
                        "to"    : 10100000
                    }
                })==";

        CursorMock cursor;
        session->query(&cursor, query);
        BOOST_REQUIRE(cursor.done);
        BOOST_REQUIRE_EQUAL(cursor.error, AKU_EQUERY_PARSING_ERROR);
    }

}
