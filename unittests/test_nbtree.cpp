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

enum class ScanDir {
    FWD, BWD
};

void test_nbtree_roots_collection(u32 N, ScanDir dir=ScanDir::FWD) {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeRootsCollection>(42, addrlist, bstore);
    for (u32 i = 0; i < N; i++) {
        collection->append(i+1, 0.5*(i+1));
    }
    // Read data back
    std::vector<aku_Timestamp> ts(N, 0);
    std::vector<double> xs(N, 0);
    std::unique_ptr<NBTreeIterator> it;
    if (dir == ScanDir::FWD) {
        it = collection->search(0, N);
    } else {
        it = collection->search(N, 0);
    }
    aku_Status status;
    size_t sz;
    std::tie(status, sz) = it->read(ts.data(), xs.data(), N);

    BOOST_REQUIRE_EQUAL(sz, N);

    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    if (dir == ScanDir::FWD) {
        for (u32 i = 0; i < N; i++) {
            const auto curr = i + 1;
            if (ts[i] != curr) {
                BOOST_FAIL("Invalid timestamp at " << curr << ", actual: " << ts[i]);
            }
            if (xs[i] != 0.5*curr) {
                BOOST_FAIL("Invalid value at " << curr << ", expected: " << (0.5*curr) << ", actual: " << xs[i]);
            }
        }
    } else {
        for (u32 i = 0; i < N; i++) {
            const auto max = N;
            if (ts[i] != (max - i)) {
                BOOST_FAIL("Invalid timestamp at " << (max - i) << ", actual: " << ts[i]);
            }
            if (xs[i] != 0.5*(max - i)) {
                BOOST_FAIL("Invalid value at " << (max - i) << ", expected: " << (0.5*(max - i)) << ", actual: " << xs[i]);
            }
        }

    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_1) {
    test_nbtree_roots_collection(100, ScanDir::FWD);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_2) {
    test_nbtree_roots_collection(2000, ScanDir::FWD);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_3) {
    test_nbtree_roots_collection(200000, ScanDir::FWD);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_4) {
    test_nbtree_roots_collection(100, ScanDir::BWD);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_5) {
    test_nbtree_roots_collection(2000, ScanDir::BWD);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_6) {
    test_nbtree_roots_collection(200000, ScanDir::BWD);
}
