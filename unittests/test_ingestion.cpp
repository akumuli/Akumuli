#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>

#include <sqlite3.h>
#include <apr.h>
#include <apr_dbd.h>

#include "akumuli.h"
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"
#include "ingestion_engine/ingestion_engine.h"
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
using namespace Akumuli::Ingress;

std::unique_ptr<MetadataStorage> create_metadatastorage() {
    // Create in-memory sqlite database.
    std::unique_ptr<MetadataStorage> meta;
    meta.reset(new MetadataStorage(":memory:"));
    return std::move(meta);
}

BOOST_AUTO_TEST_CASE(Test_ingress_create) {
    // Do nothing, just create all the things
    auto meta = create_metadatastorage();
    std::shared_ptr<TreeRegistry> registry = std::make_shared<TreeRegistry>(std::move(meta));
    auto dispatcher = registry->create_dispatcher();
}
