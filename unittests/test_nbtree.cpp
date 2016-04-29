#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>

#include <apr.h>
#include "akumuli.h"
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"
#include "log_iface.h"

void test_logger(aku_LogLevel tag, const char* msg) {
    BOOST_MESSAGE(msg);
}

struct AkumuliInitializer {
    AkumuliInitializer() {
        apr_initialize();
        Akumuli::Logger::set_logger(&test_logger);
    }
};

AkumuliInitializer initializer;

using namespace Akumuli;
using namespace Akumuli::StorageEngine;


static const std::vector<u32> CAPACITIES = { 8, 8 };  // two 64KB volumes
static const std::vector<std::string> VOLPATH = { "volume0", "volume1" };
static const std::string METAPATH = "metavolume";


static void create_blockstore() {
    Volume::create_new(VOLPATH[0].c_str(), CAPACITIES[0]);
    Volume::create_new(VOLPATH[1].c_str(), CAPACITIES[1]);
    MetaVolume::create_new(METAPATH.c_str(), 2, CAPACITIES.data());
}

static std::shared_ptr<FixedSizeFileStorage> open_blockstore() {
    auto bstore = FixedSizeFileStorage::open(METAPATH, VOLPATH);
    return bstore;
}


static void delete_blockstore() {
    apr_pool_t* pool;
    apr_pool_create(&pool, nullptr);
    apr_file_remove(METAPATH.c_str(), pool);
    apr_file_remove(VOLPATH[0].c_str(), pool);
    apr_file_remove(VOLPATH[1].c_str(), pool);
    apr_pool_destroy(pool);
}

void test_nbtree_forward(const int N) {
    delete_blockstore();
    create_blockstore();

    auto bstore = open_blockstore();
    NBTree tree(42, bstore);

    for (int i = 0; i < N; i++) {
        tree.append(i, i*0.1);
    }

    NBTreeCursor cursor(tree, 0, N);
    aku_Timestamp curr = 0ull;
    bool first = true;
    int index = 0;
    while(!cursor.is_eof()) {
        for (size_t ix = 0; ix < cursor.size(); ix++) {
            aku_Timestamp ts;
            double value;
            aku_Status status;
            std::tie(status, ts, value) = cursor.at(ix);

            BOOST_REQUIRE(status == AKU_SUCCESS);

            if (first) {
                first = false;
                curr = ts;
            }

            if (curr != ts) {
                BOOST_FAIL("Invalid timestamp, expected: " << curr  <<
                           " actual " << ts << " index " << index);
            }

            BOOST_REQUIRE_EQUAL(curr*0.1, value);

            curr++;
            index++;
        }
        cursor.proceed();
    }
    BOOST_REQUIRE_EQUAL(curr, N);

    delete_blockstore();
}

BOOST_AUTO_TEST_CASE(Test_nbtree_forward_0) {
    test_nbtree_forward(11);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_forward_1) {
    test_nbtree_forward(117);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_forward_2) {
    test_nbtree_forward(11771);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_forward_3) {
    test_nbtree_forward(100000);
}

void test_nbtree_roots_collection() {
    int N = 1000*32;
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::vector<LogicAddr> addrlist;  // should be empty at first
    NBTreeRootsCollection collection(42, addrlist, bstore);
    std::shared_ptr<NBTreeRoot> leaf = collection.lease(0);
    for (int i = 0; i < N; i++) {
        leaf->append(i, 0.5*i);
    }
    // TODO: check results when implementation will be ready
    // TODO: check start with non-empty tree
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_1) {
    test_nbtree_roots_collection();
}
