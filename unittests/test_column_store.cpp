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

BOOST_AUTO_TEST_CASE(Test_columns_store_1) {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::unique_ptr<MetadataStorage> meta;
    meta.reset(new MetadataStorage(":memory:"));
    std::shared_ptr<ColumnStore> cstore;
    cstore.reset(new ColumnStore(bstore, std::move(meta)));
}
