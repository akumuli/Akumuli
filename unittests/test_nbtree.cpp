#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>

#include <apr.h>
#include <queue>
#include <fstream>
#include <stdlib.h>

#include "akumuli.h"
#include "storage_engine/blockstore.h"
#include "storage_engine/volume.h"
#include "storage_engine/nbtree.h"
#include "log_iface.h"
#include "status_util.h"

void test_logger(aku_LogLevel tag, const char* msg) {
    AKU_UNUSED(tag);
    BOOST_TEST_MESSAGE(msg);
}

std::string to_isostring(aku_Timestamp ts) {
    if (ts == AKU_MAX_TIMESTAMP) {
        return "MAX";
    } else if (ts == AKU_MIN_TIMESTAMP) {
        return "MIN";
    }
    return std::to_string(ts);
}

struct AkumuliInitializer {
    AkumuliInitializer() {
        apr_initialize();
        Akumuli::Logger::set_logger(&test_logger);
    }
};

static AkumuliInitializer initializer;

using namespace Akumuli;
using namespace Akumuli::StorageEngine;


enum class ScanDir {
    FWD, BWD
};

void test_nbtree_roots_collection(u32 N, u32 begin, u32 end) {
    ScanDir dir = begin < end ? ScanDir::FWD : ScanDir::BWD;
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();
    for (u32 i = 0; i < N; i++) {
        collection->append(i, i);
    }

    // Read data back
    std::unique_ptr<RealValuedOperator> it = collection->search(begin, end);

    aku_Status status;
    size_t sz;
    size_t outsz = dir == ScanDir::FWD ? end - begin : begin - end;
    std::vector<aku_Timestamp> ts(outsz, 0xF0F0F0F0);
    std::vector<double> xs(outsz, -1);
    std::tie(status, sz) = it->read(ts.data(), xs.data(), outsz);

    BOOST_REQUIRE_EQUAL(sz, outsz);

    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    if (dir == ScanDir::FWD) {
        for (u32 i = 0; i < outsz; i++) {
            const auto curr = i + begin;
            if (ts[i] != curr) {
                BOOST_FAIL("Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i]);
            }
            if (!same_value(xs[i], curr)) {
                BOOST_FAIL("Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i]);
            }
        }
    } else {
        for (u32 i = 0; i < outsz; i++) {
            const auto curr = begin - i;
            if (ts[i] != curr) {
                BOOST_FAIL("Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i]);
            }
            if (!same_value(xs[i], curr)) {
                BOOST_FAIL("Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i]);
            }
        }

    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_1) {
    test_nbtree_roots_collection(100, 0, 100);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_2) {
    test_nbtree_roots_collection(2000, 0, 2000);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_3) {
    test_nbtree_roots_collection(200000, 0, 200000);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_4) {
    test_nbtree_roots_collection(100, 99, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_5) {
    test_nbtree_roots_collection(2000, 1999, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_6) {
    test_nbtree_roots_collection(200000, 199999, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_rc_append_rand_read) {
    for (int i = 0; i < 100; i++) {
        auto N = static_cast<u32>(rand()) % 200000u;
        auto from = static_cast<u32>(rand()) % N;
        auto to = static_cast<u32>(rand()) % N;
        test_nbtree_roots_collection(N, from, to);
    }
}

void test_nbtree_chunked_read(u32 N, u32 begin, u32 end, u32 chunk_size) {
    ScanDir dir = begin < end ? ScanDir::FWD : ScanDir::BWD;
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    for (u32 i = 0; i < N; i++) {
        collection->append(i, i);
    }

    // Read data back
    std::unique_ptr<RealValuedOperator> it = collection->search(begin, end);

    aku_Status status;
    size_t sz;
    std::vector<aku_Timestamp> ts(chunk_size, 0xF0F0F0F0);
    std::vector<double> xs(chunk_size, -1);

    u32 total_size = 0u;
    aku_Timestamp ts_seen = begin;
    while(true) {
        std::tie(status, sz) = it->read(ts.data(), xs.data(), chunk_size);

        if (sz == 0 && status == AKU_SUCCESS) {
            BOOST_FAIL("Invalid iterator output, sz=0, status=" << status);
        }
        total_size += sz;

        BOOST_REQUIRE(status == AKU_SUCCESS || status == AKU_ENO_DATA);

        if (dir == ScanDir::FWD) {
            for (u32 i = 0; i < sz; i++) {
                const auto curr = ts_seen;
                if (ts[i] != curr) {
                    BOOST_FAIL("Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i]);
                }
                if (!same_value(xs[i], curr)) {
                    BOOST_FAIL("Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i]);
                }
                ts_seen = ts[i] + 1;
            }
        } else {
            for (u32 i = 0; i < sz; i++) {
                const auto curr = ts_seen;
                if (ts[i] != curr) {
                    BOOST_FAIL("Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i]);
                }
                if (!same_value(xs[i], curr)) {
                    BOOST_FAIL("Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i]);
                }
                ts_seen = ts[i] - 1;
            }
        }

        if (status == AKU_ENO_DATA || ts_seen == end) {
            break;
        }
    }
    if (ts_seen != end) {
        BOOST_FAIL("Bad range, expected: " << end << ", actual: " << ts_seen <<
                   " dir: " << (dir == ScanDir::FWD ? "forward" : "backward"));
    }
    size_t outsz = dir == ScanDir::FWD ? end - begin : begin - end;
    BOOST_REQUIRE_EQUAL(total_size, outsz);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_chunked_read) {
    for (u32 i = 0; i < 100; i++) {
        auto N = static_cast<u32>(rand() % 200000);
        auto from = static_cast<u32>(rand()) % N;
        auto to = static_cast<u32>(rand()) % N;
        auto chunk = static_cast<u32>(rand()) % N;
        test_nbtree_chunked_read(N, from, to, chunk);
    }
}

void check_tree_consistency(std::shared_ptr<BlockStore> bstore, size_t level, NBTreeExtent const* extent) {
    NBTreeExtent::check_extent(extent, bstore, level);
}

void test_reopen_storage(i32 Npages, i32 Nitems) {
    LogicAddr last_one = EMPTY_ADDR;
    std::shared_ptr<BlockStore> bstore =
        BlockStoreBuilder::create_memstore([&last_one](LogicAddr addr) { last_one = addr; });
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    u32 nleafs = 0;
    u32 nitems = 0;
    for (u32 i = 0; true; i++) {
        if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            // addrlist changed
            auto newroots = collection->get_roots();
            if (newroots == addrlist) {
                BOOST_FAIL("Roots collection must change");
            }
            std::swap(newroots, addrlist);
            nleafs++;
            if (static_cast<i32>(nleafs) == Npages) {
                nitems = i;
                break;
            }
        }
        if (static_cast<i32>(i) == Nitems) {
            nitems = i;
            break;
        }
    }

    addrlist = collection->close();

    BOOST_REQUIRE_EQUAL(addrlist.back(), last_one);

    // TODO: check attempt to open tree using wrong id!
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    auto extents = collection->get_extents();
    for (size_t i = 0; i < extents.size(); i++) {
        auto extent = extents[i];
        check_tree_consistency(bstore, i, extent);
    }

    std::unique_ptr<RealValuedOperator> it = collection->search(0, nitems);
    std::vector<aku_Timestamp> ts(nitems, 0);
    std::vector<double> xs(nitems, 0);
    aku_Status status = AKU_SUCCESS;
    size_t sz = 0;
    std::tie(status, sz) = it->read(ts.data(), xs.data(), nitems);
    BOOST_REQUIRE(sz == nitems);
    BOOST_REQUIRE(status == AKU_SUCCESS);
    for (u32 i = 0; i < nitems; i++) {
        if (ts[i] != i) {
            BOOST_FAIL("Invalid timestamp at " << i);
        }
        if (!same_value(xs[i], static_cast<double>(i))) {
            BOOST_FAIL("Invalid timestamp at " << i);
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_1) {
    test_reopen_storage(-1, 1);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_2) {
    test_reopen_storage(1, -1);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_3) {
    test_reopen_storage(2, -1);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_4) {
    test_reopen_storage(32, -1);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_5) {
    test_reopen_storage(33, -1);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_reopen_6) {
    test_reopen_storage(32*32, -1);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery_status(u32 N, u32 N_values) {
    LogicAddr last_block = EMPTY_ADDR;
    auto cb = [&last_block] (LogicAddr addr) {
        last_block = addr;
    };
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    u32 nleafs = 0;
    u32 nitems = 0;
    for (u32 i = 0; true; i++) {
        if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            // addrlist changed
            auto newroots = collection->get_roots();
            if (newroots == addrlist) {
                BOOST_FAIL("Roots collection must change");
            }
            std::swap(newroots, addrlist);
            auto status = NBTreeExtentsList::repair_status(addrlist);
            BOOST_REQUIRE(status == NBTreeExtentsList::RepairStatus::REPAIR);
            nleafs++;
            if (nleafs == N) {
                nitems = i;
                break;
            }
        }
        if (i == N_values) {
            nitems = i;
            break;
        }
    }
    addrlist = collection->close();
    auto status = NBTreeExtentsList::repair_status(addrlist);
    BOOST_REQUIRE(status == NBTreeExtentsList::RepairStatus::OK);
    BOOST_REQUIRE(addrlist.back() == last_block);
    AKU_UNUSED(nitems);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_status_1) {
    test_storage_recovery_status(~0u, 32);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_status_2) {
    test_storage_recovery_status(2, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_status_3) {
    test_storage_recovery_status(32, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_status_4) {
    test_storage_recovery_status(32*32, ~0u);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery(u32 N_blocks, u32 N_values) {
    LogicAddr last_block = EMPTY_ADDR;
    auto cb = [&last_block] (LogicAddr addr) {
        last_block = addr;
    };
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    u32 nleafs = 0;
    u32 nitems = 0;
    for (u32 i = 0; true; i++) {
        if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            // addrlist changed
            auto newroots = collection->get_roots();
            if (newroots == addrlist) {
                BOOST_FAIL("Roots collection must change");
            }
            std::swap(newroots, addrlist);
            auto status = NBTreeExtentsList::repair_status(addrlist);
            BOOST_REQUIRE(status == NBTreeExtentsList::RepairStatus::REPAIR);
            nleafs++;
            if (nleafs == N_blocks) {
                nitems = i;
                break;
            }
        }
        if (i == N_values) {
            nitems = i;
            break;
        }
    }

    addrlist = collection->get_roots();

    //for (auto addr: addrlist) {
    //    std::cout << "\n\nDbg print for " << addr << std::endl;
    //    NBTreeExtentsList::debug_print(addr, bstore);
    //}

    // delete roots collection
    collection.reset();

    // TODO: check attempt to open tree using wrong id!
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    auto extents = collection->get_extents();
    for (size_t i = 0; i < extents.size(); i++) {
        auto extent = extents[i];
        check_tree_consistency(bstore, i, extent);
    }

    // Scan entire tree
    std::unique_ptr<RealValuedOperator> it = collection->search(0, nitems);
    std::vector<aku_Timestamp> ts(nitems, 0);
    std::vector<double> xs(nitems, 0);
    size_t sz = 0;
    aku_Status status;
    std::tie(status, sz) = it->read(ts.data(), xs.data(), nitems);
    if (addrlist.empty()) {
        // Expect zero, data was stored in single leaf-node.
        BOOST_REQUIRE(sz == 0);
    } else {
        if (nleafs == N_blocks) {
            // new leaf was empty before 'crash'
            BOOST_REQUIRE(sz == nitems);
        } else {
            // some data can be lost!
            BOOST_REQUIRE(sz <= nitems);
        }
    }
    // Note: `status` should be equal to AKU_SUCCESS if size of the destination
    // is equal to array's length. Otherwise iterator should return AKU_ENO_DATA
    // as an indication that all data-elements have ben read.
    BOOST_REQUIRE(status == AKU_ENO_DATA || status  == AKU_SUCCESS);
    for (u32 i = 0; i < sz; i++) {
        if (ts[i] != i) {
            BOOST_FAIL("Invalid timestamp at " << i);
        }
        if (!same_value(xs[i], static_cast<double>(i))) {
            BOOST_FAIL("Invalid timestamp at " << i);
        }
    }

    if (sz) {
        // Expected aggregates (calculated by hand)
        AggregationResult exp_agg = INIT_AGGRES;
        exp_agg.do_the_math(ts.data(), xs.data(), sz, false);

        // Single leaf node will be lost and aggregates will be empty anyway
        auto agg_iter = collection->aggregate(0, nitems);
        aku_Timestamp new_agg_ts;
        AggregationResult new_agg_result = INIT_AGGRES;
        size_t agg_size;
        std::tie(status, agg_size) = agg_iter->read(&new_agg_ts, &new_agg_result, 1);
        if (status != AKU_SUCCESS) {
            BOOST_FAIL("Can't aggregate after recovery " + StatusUtil::str(status));
        }

        // Check that results are correct and match the one that was calculated by hand
        BOOST_REQUIRE_EQUAL(new_agg_result.cnt, exp_agg.cnt);
        BOOST_REQUIRE_EQUAL(new_agg_result.first, exp_agg.first);
        BOOST_REQUIRE_EQUAL(new_agg_result.last, exp_agg.last);
        BOOST_REQUIRE_EQUAL(new_agg_result.max, exp_agg.max);
        BOOST_REQUIRE_EQUAL(new_agg_result.maxts, exp_agg.maxts);
        BOOST_REQUIRE_EQUAL(new_agg_result.min, exp_agg.min);
        BOOST_REQUIRE_EQUAL(new_agg_result.mints, exp_agg.mints);
        BOOST_REQUIRE_EQUAL(new_agg_result.sum, exp_agg.sum);
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_1) {
    test_storage_recovery(~0u, 10);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_2) {
    test_storage_recovery(1, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_3) {
    test_storage_recovery(31, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_4) {
    test_storage_recovery(32, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_5) {
    test_storage_recovery(33, ~0u);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_6) {
    test_storage_recovery(33*33, ~0u);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery_2(u32 N_blocks) {
    LogicAddr last_block = EMPTY_ADDR;
    auto cb = [&last_block] (LogicAddr addr) {
        last_block = addr;
    };
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
    std::vector<LogicAddr> addrlist;  // should be empty at first
    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    u32 nleafs = 0;

    auto try_to_recover = [&](std::vector<LogicAddr>&& addrlist, u32 N) {
        auto col = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
        col->force_init();
        // scan
        auto it = col->search(0, N);
        std::vector<aku_Timestamp> ts(N, 0);
        std::vector<double> xs(N, 0);
        aku_Status status = AKU_SUCCESS;
        size_t sz = 0;
        std::tie(status, sz) = it->read(ts.data(), xs.data(), N+1);
        BOOST_REQUIRE(sz == N);
        BOOST_REQUIRE(status == AKU_ENO_DATA || status  == AKU_SUCCESS);
        if (sz > 0) {
            BOOST_REQUIRE(ts[0] == 0);
            BOOST_REQUIRE(ts[sz - 1] == sz - 1);
        }

        if (sz) {
            auto agg_iter = col->aggregate(0, N+1);
            aku_Timestamp agg_ts;
            AggregationResult act_agg = INIT_AGGRES;
            size_t agg_size;
            std::tie(status, agg_size) = agg_iter->read(&agg_ts, &act_agg, 1);
            if (status != AKU_SUCCESS) {
                BOOST_FAIL("Can't aggregate after recovery " + StatusUtil::str(status));
            }

            // Check that results are correct and match the one that was calculated by hand
            double exp_sum = (static_cast<double>(N - 1) * N) / 2.0;  // sum of the arithmetic progression [0:N-1]
            BOOST_REQUIRE_EQUAL(act_agg.cnt, N);
            BOOST_REQUIRE_EQUAL(act_agg.first, 0);
            BOOST_REQUIRE_EQUAL(act_agg.last, N-1);
            BOOST_REQUIRE_EQUAL(act_agg.max, N-1);
            BOOST_REQUIRE_EQUAL(act_agg.maxts, N-1);
            BOOST_REQUIRE_EQUAL(act_agg.min, 0);
            BOOST_REQUIRE_EQUAL(act_agg.mints, 0);
            BOOST_REQUIRE_EQUAL(act_agg.sum, exp_sum);
        }
    };

    for (u32 i = 0; true; i++) {
        if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            // addrlist changed
            if (nleafs % 10 == 0) {
                try_to_recover(collection->get_roots(), i);
            }
            nleafs++;
            if (nleafs == N_blocks) {
                break;
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_7) {
    test_storage_recovery_2(32*32);
}


// Test iteration

void test_nbtree_leaf_iteration(aku_Timestamp begin, aku_Timestamp end) {
    NBTreeLeaf leaf(42, EMPTY_ADDR, 0);
    aku_Timestamp last_successfull = 100;
    aku_Timestamp first_timestamp = 100;
    for (size_t ix = first_timestamp; true; ix++) {
        aku_Status status = leaf.append(ix, static_cast<double>(ix));
        if (status == AKU_EOVERFLOW) {
            break;
        }
        if (status == AKU_SUCCESS) {
            last_successfull = ix;
            continue;
        }
        BOOST_FAIL(StatusUtil::c_str(status));
    }
    // Everytithing should work before commit
    auto iter = leaf.range(begin, end);
    // Calculate output size
    size_t sz = 0;
    auto min = std::min(begin, end);
    min = std::max(min, first_timestamp);
    auto max = std::max(begin, end);
    max = std::min(max, last_successfull);
    sz = max - min;
    // Perform read using iterator
    std::vector<aku_Timestamp> tss(sz, 0);
    std::vector<double> xss(sz, 0);
    aku_Status status;
    size_t outsz;
    std::tie(status, outsz) = iter->read(tss.data(), xss.data(), sz);
    // Check results
    BOOST_REQUIRE_EQUAL(outsz, sz);
    if(status != AKU_SUCCESS) {
        BOOST_FAIL(StatusUtil::c_str(status));
    }
    if (end < begin) {
        std::reverse(tss.begin(), tss.end());
        std::reverse(xss.begin(), xss.end());
        min++;
    }
    for(size_t ix = 0; ix < sz; ix++) {
        // iter from min to max
        BOOST_REQUIRE_EQUAL(tss.at(ix), min);
        BOOST_REQUIRE_EQUAL(xss.at(ix), static_cast<double>(min));
        min++;
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_1) {
    test_nbtree_leaf_iteration(0, 100000000);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_2) {
    test_nbtree_leaf_iteration(100000000, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_3) {
    test_nbtree_leaf_iteration(200, 100000000);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_4) {
    test_nbtree_leaf_iteration(100000000, 200);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_5) {
    test_nbtree_leaf_iteration(0, 500);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_6) {
    test_nbtree_leaf_iteration(500, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_7) {
    test_nbtree_leaf_iteration(200, 500);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_iteration_8) {
    test_nbtree_leaf_iteration(500, 200);
}

// Test aggregation

//! Generate time-series from random walk
struct RandomWalk {
    std::random_device                  randdev;
    std::mt19937                        generator;
    std::normal_distribution<double>    distribution;
    double                              value;

    RandomWalk(double start, double mean, double stddev)
        : generator(randdev())
        , distribution(mean, stddev)
        , value(start)
    {
    }

    double next() {
        value += distribution(generator);
        return value;
    }
};

AggregationResult calculate_expected_value(std::vector<double> const& xss) {
    AggregationResult expected = INIT_AGGRES;
    expected.sum = std::accumulate(xss.begin(), xss.end(), 0.0, [](double a, double b) { return a + b; });
    expected.max = std::accumulate(xss.begin(), xss.end(), std::numeric_limits<double>::min(),
                        [](double a, double b) {
                            return std::max(a, b);
                        });
    expected.min = std::accumulate(xss.begin(), xss.end(), std::numeric_limits<double>::max(),
                        [](double a, double b) {
                            return std::min(a, b);
                        });
    expected.cnt = xss.size();
    return expected;
}

void test_nbtree_leaf_aggregation(aku_Timestamp begin, aku_Timestamp end) {
    NBTreeLeaf leaf(42, EMPTY_ADDR, 0);
    aku_Timestamp first_timestamp = 100;
    std::vector<double> xss;
    RandomWalk rwalk(0.0, 1.0, 1.0);
    for (size_t ix = first_timestamp; true; ix++) {
        double val = rwalk.next();
        aku_Status status = leaf.append(ix, val);
        if (status == AKU_EOVERFLOW) {
            break;
        }
        if (status == AKU_SUCCESS) {
            if (begin < end) {
                if (ix >= begin && ix < end) {
                    xss.push_back(val);
                }
            } else {
                if (ix <= begin && ix > end) {
                    xss.push_back(val);
                }
            }
            continue;
        }
        BOOST_FAIL(StatusUtil::c_str(status));
    }
    double first = xss.front();
    double last = xss.back();
    if (end < begin) {
        // we should reverse xss, otherwise expected and actual values wouldn't match exactly because
        // floating point arithmetics is not commutative
        std::reverse(xss.begin(), xss.end());
    }

    // Compute expected value
    auto expected = calculate_expected_value(xss);

    // Compare expected and actual
    auto it = leaf.aggregate(begin, end);
    aku_Status status;
    size_t size = 100;
    std::vector<aku_Timestamp> destts(size, 0);
    std::vector<AggregationResult> destxs(size, INIT_AGGRES);
    size_t outsz;
    std::tie(status, outsz) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(outsz, 1);

    auto actual = destxs.at(0);
    BOOST_REQUIRE_CLOSE(actual.cnt, expected.cnt, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.sum, expected.sum, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.min, expected.min, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.max, expected.max, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.first,      first, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.last,        last, 10e-5);

    // Subsequent call to `it->read` should fail
    std::tie(status, outsz) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(status, AKU_ENO_DATA);
    BOOST_REQUIRE_EQUAL(outsz, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_leaf_aggregation) {
    std::vector<std::pair<aku_Timestamp, aku_Timestamp>> params = {
    // Fwd
        {  0,   10000000},
        {200,   10000000},
        {  0,        300},
        {200,        400},
    // Bwd
        {10000000,     0},
        {10000000,   200},
        {     300,     0},
        {     400,   200},
    };
    for (auto cp: params) {
        test_nbtree_leaf_aggregation(cp.first, cp.second);
    }
}

void test_nbtree_superblock_iter(aku_Timestamp begin, aku_Timestamp end) {
    // Build this tree structure.
    aku_Timestamp gen = 1000;
    size_t ncommits = 0;
    auto commit_counter = [&ncommits](LogicAddr) {
        ncommits++;
    };
    std::vector<double> expected;
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();
    RandomWalk rwalk(1.0, 0.1, 0.1);
    while(ncommits < AKU_NBTREE_FANOUT*AKU_NBTREE_FANOUT) {  // we should build three levels
        double value = rwalk.next();
        aku_Timestamp ts = gen++;
        extents->append(ts, value);
        if (begin < end) {
            if (ts >= begin && ts < end) {
                expected.push_back(value);
            }
        } else {
            if (ts <= begin && ts > end) {
                expected.push_back(value);
            }
        }
    }
    if (begin > end) {
        std::reverse(expected.begin(), expected.end());
    }
    // Check actual output
    auto it = extents->search(begin, end);
    size_t chunk_size = 1000;
    std::vector<double> destxs(chunk_size, 0);
    std::vector<aku_Timestamp> destts(chunk_size, 0);
    ssize_t expix = 0;
    while(true) {
        aku_Status status;
        ssize_t size;
        std::tie(status, size) = it->read(destts.data(), destxs.data(), chunk_size);
        if (status == AKU_ENO_DATA && size == 0) {
            BOOST_REQUIRE_EQUAL(expix, expected.size());
            break;
        }
        if (status == AKU_SUCCESS || (status == AKU_ENO_DATA && size != 0)) {
            BOOST_REQUIRE_EQUAL_COLLECTIONS(
                expected.begin() + expix, expected.begin() + expix + size,
                destxs.begin(), destxs.begin() + size
            );
            expix += size;
            continue;
        }
        BOOST_FAIL(StatusUtil::c_str(status));
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_superblock_iteration) {
    std::vector<std::pair<aku_Timestamp, aku_Timestamp>> tss = {
        {      0, 1000000 },
        {   2000, 1000000 },
        {      0,  600000 },
        {   2000,  600000 },
        { 400000,  500000 },
    };
    for (auto be: tss) {
        test_nbtree_superblock_iter(be.first, be.second);
        test_nbtree_superblock_iter(be.second, be.first);
    }
}

void test_nbtree_superblock_aggregation(aku_Timestamp begin, aku_Timestamp end) {
    // Build this tree structure.
    aku_Timestamp gen = 1000;
    size_t ncommits = 0;
    auto commit_counter = [&ncommits](LogicAddr) {
        ncommits++;
    };
    std::vector<double> xss;
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();
    RandomWalk rwalk(1.0, 0.1, 0.1);
    while(ncommits < AKU_NBTREE_FANOUT*AKU_NBTREE_FANOUT || gen <= 1000000ul) {  // we should build three levels
        double value = rwalk.next();
        aku_Timestamp ts = gen++;
        extents->append(ts, value);
        if (begin < end) {
            if (ts >= begin && ts < end) {
                xss.push_back(value);
            }
        } else {
            if (ts <= begin && ts > end) {
                xss.push_back(value);
            }
        }
    }
    double first = xss.empty() ? 0 : xss.front();
    double last  = xss.empty() ? 0 : xss.back();
    if (begin > end) {
        std::reverse(xss.begin(), xss.end());
    }
    auto expected = calculate_expected_value(xss);

    // Check actual output
    auto it = extents->aggregate(begin, end);
    aku_Status status;
    size_t size = 100;
    std::vector<aku_Timestamp> destts(size, 0);
    std::vector<AggregationResult> destxs(size, INIT_AGGRES);
    std::tie(status, size) = it->read(destts.data(), destxs.data(), size);

    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(size, 1);

    auto actual = destxs.at(0);
    BOOST_REQUIRE_CLOSE(actual.cnt, expected.cnt, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.sum, expected.sum, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.min, expected.min, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.max, expected.max, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.first,      first, 10e-5);
    BOOST_REQUIRE_CLOSE(actual.last,        last, 10e-5);

    // Subsequent call to `it->read` should fail
    std::tie(status, size) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(status, AKU_ENO_DATA);
    BOOST_REQUIRE_EQUAL(size, 0);
}

BOOST_AUTO_TEST_CASE(Test_nbtree_superblock_aggregation) {
    std::vector<std::pair<aku_Timestamp, aku_Timestamp>> tss = {
        {      0, 1000000 },
        {   2000, 1000000 },
        {      0,  600000 },
        {   2000,  600000 },
        { 400000,  500000 },
    };
    for (auto be: tss) {
        test_nbtree_superblock_aggregation(be.first, be.second);
        test_nbtree_superblock_aggregation(be.second, be.first);
    }
}

void test_nbtree_recovery_with_retention(LogicAddr nblocks, LogicAddr nremoved) {
    // Build this tree structure.
    assert(nremoved <= nblocks);  // both numbers are actually a numbers
    aku_Timestamp gen = 1000;
    aku_Timestamp begin = gen, end = gen, last_ts = gen;
    size_t buffer_cnt = 0;
    std::shared_ptr<NBTreeExtentsList> extents;
    auto commit_counter = [&](LogicAddr) {
        buffer_cnt++;
        if (buffer_cnt == nremoved) {
            // one time event
            begin = gen;
        }
        end = last_ts;
    };
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    extents.reset(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();
    RandomWalk rwalk(1.0, 0.1, 0.1);
    while(buffer_cnt < nblocks) {
        double value = rwalk.next();
        aku_Timestamp ts = gen++;
        extents->append(ts, value);
        last_ts = ts;
    }
    // Remove old values
    std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(nremoved);

    // Recovery
    auto rescue_points = extents->get_roots();
    // We shouldn't close `extents` to emulate program state after crush.
    std::shared_ptr<NBTreeExtentsList> recovered(new NBTreeExtentsList(42, rescue_points, bstore));
    recovered->force_init();

    auto it = recovered->search(begin, end);
    if (end > begin) {
        size_t sz = end - begin;
        std::vector<aku_Timestamp> tss(sz, 0);
        std::vector<double> xss(sz, .0);
        aku_Status stat;
        size_t outsz;
        std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
        if (outsz != sz) {
            BOOST_REQUIRE_EQUAL(outsz, sz);
        }
        if (stat != AKU_SUCCESS && stat != AKU_ENO_DATA) {
            BOOST_REQUIRE(stat == AKU_SUCCESS || stat == AKU_ENO_DATA);
        }
        for(aku_Timestamp ts: tss) {
            BOOST_REQUIRE_EQUAL(ts, begin);
            begin++;
        }
    } else {
        // No output expected
        size_t sz = 10;
        std::vector<aku_Timestamp> tss(sz, 0);
        std::vector<double> xss(sz, .0);
        aku_Status stat;
        size_t outsz;
        std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
        if (outsz != 0) {
            BOOST_REQUIRE_EQUAL(outsz, 0);
        }
        if (stat != AKU_ENO_DATA) {
            BOOST_REQUIRE(stat == AKU_ENO_DATA);
        }
    }
}

BOOST_AUTO_TEST_CASE(Test_nbtree_recovery_with_retention_1) {
    std::vector<std::pair<LogicAddr, LogicAddr>> addrlist = {
        { 1, 0 },
        { 1, 1 },
        { 2, 0 },
        { 2, 1 },
        { 33, 1},
        { 33, 10},
        { 33, 33},
        { 33*33, 33},
        { 33*33, 33*33},
    };
    for(auto pair: addrlist) {
        test_nbtree_recovery_with_retention(pair.first, pair.second);
    }
}


void test_nbtree_superblock_candlesticks(size_t commit_limit, aku_Timestamp delta) {
    // Build this tree structure.
    aku_Timestamp begin = 1000;
    aku_Timestamp end = begin;
    size_t ncommits = 0;
    auto commit_counter = [&ncommits](LogicAddr) {
        ncommits++;
    };
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();
    RandomWalk rwalk(1.0, 0.1, 0.1);
    while(ncommits < AKU_NBTREE_FANOUT*AKU_NBTREE_FANOUT) {  // we should build three levels
        double value = rwalk.next();
        aku_Timestamp ts = end++;
        extents->append(ts, value);
    }

    // Check actual output
    NBTreeCandlestickHint hint;
    hint.min_delta = delta;
    auto it = extents->candlesticks(begin, end, hint);
    aku_Status status;
    size_t size = 1000;
    std::vector<aku_Timestamp> destts(size, 0);
    std::vector<AggregationResult> destxs(size, INIT_AGGRES);
    std::tie(status, size) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    for(size_t i = 1; i < size; i++) {
        AggregationResult prev, curr;
        prev = destxs[i - 1];
        curr = destxs[i];
        BOOST_REQUIRE_CLOSE(prev.last, curr.first, 10e-5);
    }
}


BOOST_AUTO_TEST_CASE(Test_nbtree_candlesticks) {
    std::vector<std::pair<size_t, aku_Timestamp>> cases = {
        { 1, 10 },
        {10, 10 },
        {10, 10000 },
        {33, 10 },
        {33, 100 },
        {33, 1000 },
        {33, 100000 },
        {33*33, 10000 },
    };
    for (auto it: cases) {
        test_nbtree_superblock_candlesticks(it.first, it.second);
    }
}

// Check that subsequent reopen procedures doesn't increases file size
BOOST_AUTO_TEST_CASE(Test_reopen_storage_twice) {
    std::vector<LogicAddr> addrlist;
    std::shared_ptr<BlockStore> bstore =
        BlockStoreBuilder::create_memstore();

    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    std::vector<aku_Timestamp> tss = {
        1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
        1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
    };
    std::vector<double> xss = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 0
    };

    for (u32 i = 0; i < xss.size(); i++) {
        collection->append(tss[i], xss[i]);
    }

    // Close first time
    addrlist = collection->close();

    BOOST_REQUIRE_EQUAL(addrlist.size(), 1);

    // Reopen first time (this will change tree configuration from single leaf node to superblock+leaf)
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    auto extents = collection->get_extents();
    for (size_t i = 0; i < extents.size(); i++) {
        auto extent = extents[i];
        check_tree_consistency(bstore, i, extent);
    }

    // Close second time
    auto addrlist2 = collection->close();

    BOOST_REQUIRE_EQUAL(addrlist2.size(), 1);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(addrlist.begin(), addrlist.end(), addrlist2.begin(), addrlist2.end());

    // Reopen second time (this should preserve 'superblock+leaf' tree configuration)
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist2, bstore);
    collection->force_init();

    extents = collection->get_extents();
    for (size_t i = 0; i < extents.size(); i++) {
        auto extent = extents[i];
        check_tree_consistency(bstore, i, extent);
    }
}

// Check that late write is not possible after reopen
BOOST_AUTO_TEST_CASE(Test_reopen_late_write) {
    std::vector<LogicAddr> addrlist;
    std::shared_ptr<BlockStore> bstore =
        BlockStoreBuilder::create_memstore();

    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    std::vector<aku_Timestamp> tss = {
        1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
        1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
    };
    std::vector<double> xss = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 0
    };

    for (u32 i = 0; i < xss.size(); i++) {
        collection->append(tss[i], xss[i]);
    }

    // Close first time
    addrlist = collection->close();
    BOOST_REQUIRE_EQUAL(addrlist.size(), 1);

    // Reopen first time
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    // Late write
    auto status = collection->append(tss.front(), xss.front());

    BOOST_REQUIRE(status == NBTreeAppendResult::FAIL_LATE_WRITE);
}

BOOST_AUTO_TEST_CASE(Test_reopen_write_reopen) {
    std::vector<LogicAddr> addrlist;
    std::shared_ptr<BlockStore> bstore =
        BlockStoreBuilder::create_memstore();

    auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    std::vector<aku_Timestamp> tss = {
        1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
        1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
    };
    std::vector<double> xss = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 0
    };

    for (u32 i = 0; i < xss.size(); i++) {
        collection->append(tss[i], xss[i]);
    }

    // Close first time
    addrlist = collection->close();

    BOOST_REQUIRE_EQUAL(addrlist.size(), 1);

    // Reopen first time (this will change tree configuration from single leaf node to superblock+leaf)
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    collection->force_init();

    std::vector<aku_Timestamp> tss2 = {
        1010ul, 1011ul, 1012ul, 1013ul, 1014ul
    };
    std::vector<double> xss2 = {
        1, 2, 3, 4, 5
    };
    for (u32 i = 0; i < xss2.size(); i++) {
        collection->append(tss2[i], xss2[i]);
    }

    // Close second time
    auto addrlist2 = collection->close();

    BOOST_REQUIRE_EQUAL(addrlist2.size(), 2);

    // Reopen second time
    collection = std::make_shared<NBTreeExtentsList>(42, addrlist2, bstore);
    collection->force_init();

    auto extents = collection->get_extents();
    for (size_t i = 0; i < extents.size(); i++) {
        auto extent = extents[i];
        check_tree_consistency(bstore, i, extent);
    }
}


void test_nbtree_group_aggregate_forward(size_t commit_limit, u64 step, int start_offset, const int ts_increment=1) {
    // Build this tree structure.
    aku_Timestamp begin = 1000;
    aku_Timestamp end = begin;
    size_t ncommits = 0;
    auto commit_counter = [&ncommits](LogicAddr) {
        ncommits++;
    };
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();
    RandomWalk rwalk(1.0, 0.1, 0.1);
    AggregationResult acc = INIT_AGGRES;
    std::vector<AggregationResult> buckets;
    auto query_begin = static_cast<u64>(static_cast<i64>(begin) + start_offset);
    auto bucket_ix = 0ull;
    while(ncommits < commit_limit) {
        auto current_bucket = (end - query_begin) / step;
        if (end >= query_begin && current_bucket > bucket_ix && acc.cnt) {
            bucket_ix = current_bucket;
            buckets.push_back(acc);
            acc = INIT_AGGRES;
        }
        double value = rwalk.next();
        aku_Timestamp ts = end;
        end += ts_increment;
        extents->append(ts, value);
        if (ts >= query_begin) {
            acc.add(ts, value, true);
        }
    }
    if (acc.cnt > 0) {
        buckets.push_back(acc);
    }

    // Check actual output
    auto it = extents->group_aggregate(query_begin, end, step);
    aku_Status status;
    size_t size = buckets.size();
    std::vector<aku_Timestamp> destts(size, 0);
    std::vector<AggregationResult> destxs(size, INIT_AGGRES);
    size_t out_size;
    std::tie(status, out_size) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(out_size, buckets.size());
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    for(size_t i = 1; i < size; i++) {
        BOOST_REQUIRE(destts.at(i) >= query_begin);
        BOOST_REQUIRE_CLOSE(buckets.at(i).sum, destxs.at(i).sum, 1E-10);
        BOOST_REQUIRE_CLOSE(buckets.at(i).cnt, destxs.at(i).cnt, 1E-10);
        BOOST_REQUIRE_CLOSE(buckets.at(i).min, destxs.at(i).min, 1E-10);
        BOOST_REQUIRE_CLOSE(buckets.at(i).max, destxs.at(i).max, 1E-10);
        BOOST_REQUIRE_EQUAL(buckets.at(i)._begin, destxs.at(i)._begin);
        BOOST_REQUIRE_EQUAL(buckets.at(i)._end, destxs.at(i)._end);
        BOOST_REQUIRE_EQUAL(buckets.at(i).mints, destxs.at(i).mints);
        BOOST_REQUIRE_EQUAL(buckets.at(i).maxts, destxs.at(i).maxts);
    }
}

BOOST_AUTO_TEST_CASE(Test_group_aggregate_forward) {
    const int N_runs = 1;
    std::vector<std::tuple<u32, u32, int, int>> cases = {
        std::make_tuple( 1,     100, 0, 1),
        std::make_tuple( 2,     100, 0, 1),
        std::make_tuple(10,     100, 0, 1),
        std::make_tuple(32,     100, 0, 1),
        std::make_tuple(32*32,  100, 0, 1),
        std::make_tuple(32*32,  100, 1, 1),
        std::make_tuple(32*32,  100,-1, 1),

        std::make_tuple( 1,    1000, 0, 1),
        std::make_tuple( 2,    1000, 0, 1),
        std::make_tuple(10,    1000, 0, 1),
        std::make_tuple(32,    1000, 0, 1),
        std::make_tuple(32*32, 1000, 0, 1),
        std::make_tuple(32*32, 1000, 1, 1),
        std::make_tuple(32*32, 1000,-1, 1),

        std::make_tuple( 1,   10000, 0, 1),
        std::make_tuple( 2,   10000, 0, 1),
        std::make_tuple(10,   10000, 0, 1),
        std::make_tuple(32,   10000, 0, 1),
        std::make_tuple(32*32,10000, 0, 1),
        std::make_tuple(32*32,10000, 1, 1),
        std::make_tuple(32*32,10000,-1, 1),

        std::make_tuple(    1, 100, 0, 100),
        std::make_tuple(   10, 100, 0, 100),
        std::make_tuple(   32, 100, 0, 100),
        std::make_tuple(32*32, 100, 0, 100),
        std::make_tuple(32*32, 100, 1, 100),
        std::make_tuple(32*32, 100,-1, 100),

        std::make_tuple(    1, 100, 0, 1000),
        std::make_tuple(   10, 100, 0, 1000),
        std::make_tuple(   32, 100, 0, 1000),
        std::make_tuple(32*32, 100, 0, 1000),
        std::make_tuple(32*32, 100, 1, 1000),
        std::make_tuple(32*32, 100,-1, 1000),
    };
    for (int i = 0; i < N_runs; i++) {
        for (auto t: cases) {
            test_nbtree_group_aggregate_forward(std::get<0>(t), std::get<1>(t), std::get<2>(t), std::get<3>(t));
        }
    }
}

void test_nbtree_group_aggregate_backward(size_t commit_limit, u64 step, int start_offset, const int ts_inc=1) {
    // Build this tree structure.
    aku_Timestamp begin = 1000;
    aku_Timestamp end = begin;
    size_t ncommits = 0;
    auto commit_counter = [&ncommits](LogicAddr) {
        ncommits++;
    };
    auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
    std::vector<LogicAddr> empty;
    std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
    extents->force_init();

    // Original values
    RandomWalk rwalk(1.0, 0.1, 0.1);
    std::vector<aku_Timestamp> tss;
    std::vector<double> xss;
    while(ncommits < commit_limit) {
        double value = rwalk.next();
        aku_Timestamp ts = end;
        end += ts_inc;
        extents->append(ts, value);
        tss.push_back(ts);
        xss.push_back(value);
    }

    // Calculate aggregates in backward direction
    std::reverse(xss.begin(), xss.end());
    std::reverse(tss.begin(), tss.end());
    AggregationResult acc = INIT_AGGRES;
    std::vector<AggregationResult> buckets;
    auto bucket_ix = 0ull;
    auto query_begin = static_cast<u64>(static_cast<i64>(end) - start_offset);
    auto query_end = static_cast<u64>(static_cast<i64>(begin) + start_offset);
    for (auto ix = 0ul; ix < xss.size(); ix++) {
        auto current_bucket = (query_begin - tss[ix]) / step;
        if (tss[ix] <= query_begin && tss[ix] > query_end && current_bucket != bucket_ix && acc.cnt) {
            bucket_ix = current_bucket;
            buckets.push_back(acc);
            acc = INIT_AGGRES;
        }
        if (tss[ix] <= query_begin && tss[ix] > query_end) {
            acc.add(tss[ix], xss[ix], false);
        }
    }
    if (acc.cnt > 0) {
        buckets.push_back(acc);
    }

    // Check actual output
    auto it = extents->group_aggregate(query_begin, query_end, step);
    aku_Status status;
    size_t size = buckets.size();
    std::vector<aku_Timestamp> destts(size, 0);
    std::vector<AggregationResult> destxs(size, INIT_AGGRES);
    size_t out_size;
    std::tie(status, out_size) = it->read(destts.data(), destxs.data(), size);
    BOOST_REQUIRE_EQUAL(out_size, buckets.size());
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    for(size_t i = 0; i < size; i++) {
        if (!(destts.at(i) > query_end && destts.at(i) <= query_begin)) {
            BOOST_REQUIRE(destts.at(i) > query_end && destts.at(i) <= query_begin);
        }
        if (std::abs(buckets.at(i).sum - destxs.at(i).sum) > 1e-5) {
            BOOST_REQUIRE_CLOSE(buckets.at(i).sum, destxs.at(i).sum, 1E-5);
        }
        BOOST_REQUIRE_CLOSE(buckets.at(i).cnt, destxs.at(i).cnt, 1E-5);
        BOOST_REQUIRE_CLOSE(buckets.at(i).min, destxs.at(i).min, 1E-5);
        BOOST_REQUIRE_CLOSE(buckets.at(i).max, destxs.at(i).max, 1E-5);
        BOOST_REQUIRE_EQUAL(buckets.at(i)._begin, destxs.at(i)._begin);
        BOOST_REQUIRE_EQUAL(buckets.at(i)._end, destxs.at(i)._end);
        BOOST_REQUIRE_EQUAL(buckets.at(i).mints, destxs.at(i).mints);
        BOOST_REQUIRE_EQUAL(buckets.at(i).maxts, destxs.at(i).maxts);
    }
}

BOOST_AUTO_TEST_CASE(Test_group_aggregate_backward) {
    const int N_runs = 1;
    std::vector<std::tuple<u32, u32, int, int>> cases = {
        std::make_tuple( 1,     100, 0, 1),
        std::make_tuple( 2,     100, 0, 1),
        std::make_tuple(10,     100, 0, 1),
        std::make_tuple(32,     100, 0, 1),
        std::make_tuple(32*32,  100, 0, 1),
        std::make_tuple(32*32,  100, 1, 1),
        std::make_tuple(32*32,  100,-1, 1),

        std::make_tuple( 1,    1000, 0, 1),
        std::make_tuple( 2,    1000, 0, 1),
        std::make_tuple(10,    1000, 0, 1),
        std::make_tuple(32,    1000, 0, 1),
        std::make_tuple(32*32, 1000, 0, 1),
        std::make_tuple(32*32, 1000, 1, 1),
        std::make_tuple(32*32, 1000,-1, 1),

        std::make_tuple( 1,   10000, 0, 1),
        std::make_tuple( 2,   10000, 0, 1),
        std::make_tuple(10,   10000, 0, 1),
        std::make_tuple(32,   10000, 0, 1),
        std::make_tuple(32*32,10000, 0, 1),
        std::make_tuple(32*32,10000, 1, 1),
        std::make_tuple(32*32,10000,-1, 1),

        std::make_tuple(    1, 100, 0, 100),
        std::make_tuple(   10, 100, 0, 100),
        std::make_tuple(   32, 100, 0, 100),
        std::make_tuple(32*32, 100, 0, 100),
        std::make_tuple(32*32, 100, 1, 100),
        std::make_tuple(32*32, 100,-1, 100),

        std::make_tuple(    1, 100, 0, 1000),
        std::make_tuple(   10, 100, 0, 1000),
        std::make_tuple(   32, 100, 0, 1000),
        std::make_tuple(32*32, 100, 0, 1000),
        std::make_tuple(32*32, 100, 1, 1000),
        std::make_tuple(32*32, 100,-1, 1000),
    };
    for (int i = 0; i < N_runs; i++) {
        for (auto t: cases) {
            test_nbtree_group_aggregate_backward(std::get<0>(t), std::get<1>(t), std::get<2>(t), std::get<3>(t));
        }
    }
}

template<class Cont>
static void fill_leaf(NBTreeLeaf* leaf, Cont tss) {
    for (auto ts: tss) {
        auto status = leaf->append(ts, ts*0.1);
        if (status != AKU_SUCCESS) {
            throw std::runtime_error("unexpected error");
        }
    }
}

static LogicAddr save_leaf(NBTreeLeaf* leaf, NBTreeSuperblock* parent, std::shared_ptr<BlockStore> bstore) {
    aku_Status status;
    LogicAddr  result;
    std::tie(status, result) = leaf->commit(bstore);
    if (status != AKU_SUCCESS) {
        throw std::runtime_error("commit failed");
    }
    SubtreeRef ref = {};
    status = init_subtree_from_leaf(*leaf, ref);
    if (status != AKU_SUCCESS) {
        throw std::runtime_error("can't init SubtreeRef");
    }
    ref.addr = result;
    status = parent->append(ref);
    if (status != AKU_SUCCESS) {
        throw std::runtime_error("can't append");
    }
    return result;
}

/**
 * @brief Read block from block store, expect success
 */
static std::shared_ptr<Block> read_block(std::shared_ptr<BlockStore> bstore, LogicAddr addr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(addr);
    if (status != AKU_SUCCESS) {
        throw std::runtime_error("can't read block");
    }
    return block;
}

static std::vector<aku_Timestamp> extract_timestamps(RealValuedOperator& it) {
    std::vector<aku_Timestamp> tss;
    std::vector<double> xss;
    const size_t size = 10000;
    tss.resize(size);
    xss.resize(size);
    size_t sz;
    aku_Status status;
    std::tie(status, sz) = it.read(tss.data(), xss.data(), size);
    if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
        throw std::runtime_error("can't read data");
    }
    tss.resize(sz);
    return tss;
}

/* Create the following structure:
 *          [inner]
 *         /   |   \
 *  [leaf0] [leaf1] [leaf2]
 */
void test_node_split_algorithm_lvl2(aku_Timestamp pivot, std::map<int, std::vector<aku_Timestamp>>& tss, int num_new_nodes) {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    const aku_ParamId id = 42;
    LogicAddr prev = EMPTY_ADDR;
    NBTreeSuperblock sblock(id, EMPTY_ADDR, 0, 1);
    // 0
    NBTreeLeaf l0(id, prev, 0);
    fill_leaf(&l0, tss[0]);
    prev = save_leaf(&l0, &sblock, bstore);
    // 1
    NBTreeLeaf l1(id, prev, 1);
    fill_leaf(&l1, tss[1]);
    prev = save_leaf(&l1, &sblock, bstore);
    // 2
    NBTreeLeaf l2(id, prev, 2);
    fill_leaf(&l2, tss[2]);
    prev = save_leaf(&l2, &sblock, bstore);

    LogicAddr root;
    aku_Status status;
    std::tie(status, root) = sblock.commit(bstore);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(root - prev, 1);

    LogicAddr new_root;
    LogicAddr last_child;
    std::tie(status, new_root, last_child) = sblock.split(bstore, pivot, false);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(new_root - root, num_new_nodes);

    NBTreeSuperblock new_sblock(read_block(bstore, new_root));
    std::unique_ptr<RealValuedOperator> it = new_sblock.search(0, 100, bstore);
    auto actual = extract_timestamps(*it);

    std::unique_ptr<RealValuedOperator> orig_it = sblock.search(0, 100, bstore);
    auto expected = extract_timestamps(*orig_it);

    BOOST_REQUIRE_NE(actual.size(), 0);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_1) {

    /* Split middle node in:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2]
     *
     * The result should look like this:
     *
     *          ____[inner]____
     *         /    |     |    \
     *  [leaf0] [leaf1] [leaf2] [leaf3]
     *
     * 3 new nodes should be created
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    };
    test_node_split_algorithm_lvl2(15, tss, 3);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_2) {

    /* Split first leaf node on first element in:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2]
     *
     * The result should look like this:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2]
     *
     * 2 new nodes should be created
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    };
    test_node_split_algorithm_lvl2(1, tss, 2);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_3) {

    /* Split middle node in:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2]
     *
     * The result should look like this:
     *          [inner]
     *         /   |   \ \
     *  [leaf0] [leaf1] [leaf2] [leaf3]
     *
     * 3 new nodes should be created
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    };
    test_node_split_algorithm_lvl2(30, tss, 3);
}

static LogicAddr append_inner_node(NBTreeSuperblock& root, NBTreeSuperblock& child, std::shared_ptr<BlockStore> bstore) {
    aku_Status status;
    LogicAddr child_addr;
    std::tie(status, child_addr) = child.commit(bstore);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    SubtreeRef ref;
    status = init_subtree_from_subtree(child, ref);
    ref.addr = child_addr;
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    status = root.append(ref);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    return child_addr;
}

static void check_backrefs(NBTreeSuperblock& root, std::shared_ptr<BlockStore> bstore) {
    // Get the children and check connections
    std::vector<SubtreeRef> refs;
    auto status = root.read_all(&refs);
    BOOST_REQUIRE(status == AKU_SUCCESS);
    LogicAddr prev_node_addr = EMPTY_ADDR;
    for (auto ref: refs) {
        NBTreeSuperblock curr(read_block(bstore, ref.addr));
        BOOST_REQUIRE_EQUAL(curr.get_prev_addr(), prev_node_addr);
        prev_node_addr = ref.addr;
    }
}

/* Create the following structure:
 *
 *                   [inner0]
 *                  /        \
 *          [inner1]<---------[inner2]
 *         /   |    \            |
 *  [leaf0]<-[leaf1]<-[leaf2]  [leaf3]
 *
 * After the split the links between the inner nodes should be preserved, e.g.:
 *
 *                   [inner0]
 *                  /   |    \
 *          [inner1]<---------[inner3]
 *         /    |   \            |
 *  [leaf0]  [inn4]  [leaf3]  [leaf4]
 *          /     \
 *      [leaf1]  [leaf2]
 *
 * The node `inner1` is new so `inner2` should be cloned to update the link to `inner1`.
 */
void test_node_split_algorithm_lvl3(aku_Timestamp pivot, std::map<int, std::vector<aku_Timestamp>>& tss) {
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    aku_Status status;
    const aku_ParamId id = 42;
    LogicAddr prev = EMPTY_ADDR;
    NBTreeSuperblock inner0(id, EMPTY_ADDR, 0, 2);
    NBTreeSuperblock inner1(id, EMPTY_ADDR, 0, 1);
    // 0
    NBTreeLeaf l0(id, prev, 0);
    fill_leaf(&l0, tss[0]);
    prev = save_leaf(&l0, &inner1, bstore);
    // 1
    NBTreeLeaf l1(id, prev, 1);
    fill_leaf(&l1, tss[1]);
    prev = save_leaf(&l1, &inner1, bstore);
    // 2
    NBTreeLeaf l2(id, prev, 2);
    fill_leaf(&l2, tss[2]);
    prev = save_leaf(&l2, &inner1, bstore);
    auto inner1_addr = append_inner_node(inner0, inner1, bstore);

    NBTreeSuperblock inner2(id, inner1_addr, 1, 1);
    // 3
    NBTreeLeaf l3(id, EMPTY_ADDR, 0);
    fill_leaf(&l3, tss[3]);
    prev = save_leaf(&l3, &inner2, bstore);

    append_inner_node(inner0, inner2, bstore);

    LogicAddr inner0_addr;
    std::tie(status, inner0_addr) = inner0.commit(bstore);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    LogicAddr new_inner0_addr;
    LogicAddr last_child;
    std::tie(status, new_inner0_addr, last_child) = inner0.split(bstore, pivot, true);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    NBTreeSuperblock new_sblock(read_block(bstore, new_inner0_addr));

    // Compare scan results
    std::unique_ptr<RealValuedOperator> it = new_sblock.search(0, 100, bstore);
    auto actual = extract_timestamps(*it);
    std::unique_ptr<RealValuedOperator> orig_it = inner0.search(0, 100, bstore);
    auto expected = extract_timestamps(*orig_it);
    BOOST_REQUIRE_NE(actual.size(), 0);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());

    // Compare aggregation results
    std::unique_ptr<AggregateOperator> new_agg = new_sblock.aggregate(0, 100, bstore);
    std::unique_ptr<AggregateOperator> old_agg = inner0.aggregate(0, 100, bstore);
    AggregationResult new_agg_res;
    aku_Timestamp new_agg_ts;
    size_t size;
    std::tie(status, size) = new_agg->read(&new_agg_ts, &new_agg_res, 1);
    BOOST_REQUIRE_EQUAL(size, 1);
    BOOST_REQUIRE(status == AKU_SUCCESS || status == AKU_ENO_DATA);
    AggregationResult old_agg_res;
    aku_Timestamp old_agg_ts;
    std::tie(status, size) = old_agg->read(&old_agg_ts, &old_agg_res, 1);
    BOOST_REQUIRE_EQUAL(size, 1);
    BOOST_REQUIRE(status == AKU_SUCCESS || status == AKU_ENO_DATA);

    BOOST_REQUIRE_EQUAL(old_agg_res.cnt,   new_agg_res.cnt);
    BOOST_REQUIRE_EQUAL(old_agg_res.first, new_agg_res.first);
    BOOST_REQUIRE_EQUAL(old_agg_res.last,  new_agg_res.last);
    BOOST_REQUIRE_EQUAL(old_agg_res.max,   new_agg_res.max);
    BOOST_REQUIRE_EQUAL(old_agg_res.min,   new_agg_res.min);
    BOOST_REQUIRE_EQUAL(old_agg_res.maxts, new_agg_res.maxts);
    BOOST_REQUIRE_EQUAL(old_agg_res.mints, new_agg_res.mints);
    BOOST_REQUIRE_EQUAL(old_agg_res.sum,   new_agg_res.sum);

    // Check the old root first
    check_backrefs(inner0, bstore);
    check_backrefs(new_sblock, bstore);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_10) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [inner3] [leaf3]    [leaf4]
     *           /   \
     *       [leaf1] [leaf2]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(15, tss);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_11) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [inner3] [leaf1] [leaf2]    [leaf3]
     *     |
     *  [leaf0]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(1, tss);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_12) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [inner3]    [leaf4]
     *                   /   \
     *              [leaf2] [leaf3]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(25, tss);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_13) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [inner3]    [leaf4]
     *                   /   \
     *              [leaf2] [leaf3]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(30, tss);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_14) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [inner3]
     *                              /   \
     *                         [leaf3] [leaf4]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(36, tss);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_15) {

    /* Split middle node in:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [leaf3]
     *
     * The result should look like this:
     *
     *                   [inner0]
     *                  /        \
     *         [inner1]<----------[inner2]
     *         /   |   \              |
     *  [leaf0] [leaf1] [leaf2]    [inner3]
     *                                |
     *                             [leaf3]
     *
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
        { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
    };
    test_node_split_algorithm_lvl3(31, tss);
}

static std::tuple<int, int> count_nbtree_nodes(std::shared_ptr<BlockStore> bstore, LogicAddr root_addr) {
    std::queue<std::pair<LogicAddr, bool>> addrlist;
    addrlist.push(std::make_pair(root_addr, false));
    int number_of_inner_nodes = 0;
    int number_of_leaf_nodes = 0;
    while(!addrlist.empty()) {
        bool is_leaf;
        LogicAddr addr;
        std::tie(addr, is_leaf) = addrlist.front();
        addrlist.pop();
        if (is_leaf) {
            number_of_leaf_nodes++;
        } else {
            number_of_inner_nodes++;
            NBTreeSuperblock sblock(read_block(bstore, addr));
            std::vector<SubtreeRef> refs;
            auto status = sblock.read_all(&refs);
            BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
            for (auto r: refs) {
                LogicAddr a = r.addr;
                bool isl = r.type == NBTreeBlockType::LEAF;
                addrlist.push(std::make_pair(a, isl));
            }
        }
    }
    return std::tie(number_of_inner_nodes, number_of_leaf_nodes);
}

/**
 * @brief Test node split in the case when the node is being split twice
 * @param pivot1 is the first pivot point (when the first split occurs)
 * @param pivot2 is the second pivot point
 * @param tss
 */
void test_node_split_algorithm_lvl2_split_twice(aku_Timestamp pivot1,
                                                aku_Timestamp pivot2,
                                                std::map<int, std::vector<aku_Timestamp>>& tss,
                                                int expected_inner_nodes,
                                                int expected_leaf_nodes
                                                )
{
    std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
    const aku_ParamId id = 42;
    LogicAddr prev = EMPTY_ADDR;
    NBTreeSuperblock sblock(id, EMPTY_ADDR, 0, 1);

    for(auto kv: tss) {
        NBTreeLeaf leaf(id, prev, static_cast<u16>(kv.first));
        fill_leaf(&leaf, kv.second);
        prev = save_leaf(&leaf, &sblock, bstore);
    }

    LogicAddr root;
    aku_Status status;
    std::tie(status, root) = sblock.commit(bstore);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_EQUAL(root - prev, 1);

    // First split

    LogicAddr new_root1;
    LogicAddr last_child1;
    std::tie(status, new_root1, last_child1) = sblock.split(bstore, pivot1, false);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_NE(new_root1 - root, 0);

    NBTreeSuperblock new_sblock1(read_block(bstore, new_root1));
    std::unique_ptr<RealValuedOperator> it = new_sblock1.search(0, 100, bstore);
    auto actual = extract_timestamps(*it);

    std::unique_ptr<RealValuedOperator> orig_it = sblock.search(0, 100, bstore);
    auto expected = extract_timestamps(*orig_it);

    BOOST_REQUIRE_NE(actual.size(), 0);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());

    // Second split

    LogicAddr new_root2;
    LogicAddr last_child2;
    std::tie(status, new_root2, last_child2) = new_sblock1.split(bstore, pivot2, false);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);
    BOOST_REQUIRE_NE(new_root2 - new_root1, 0);

    NBTreeSuperblock new_sblock2(read_block(bstore, new_root2));
    it = new_sblock2.search(0, 100, bstore);
    actual = extract_timestamps(*it);

    BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());

    // Check the structure

    int num_inner_nodes, num_leaf_nodes;
    std::tie(num_inner_nodes, num_leaf_nodes) = count_nbtree_nodes(bstore, new_root2);
    BOOST_REQUIRE_EQUAL(num_inner_nodes, expected_inner_nodes);
    BOOST_REQUIRE_EQUAL(num_leaf_nodes, expected_leaf_nodes);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_21) {

    /* Split middle node in:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2]
     *
     * The result of the first split should look like this:
     *          [ inner ]
     *         /   |   \ \
     *  [leaf0] [leaf1] [leaf2] [leaf3]
     *
     * The result of the first split should look like this:
     *          [  inner  ]
     *         /   |   \ \ \
     *  [leaf0] [leaf1] [leaf2] [leaf3] [leaf4]
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    };
    test_node_split_algorithm_lvl2_split_twice(15, 17, tss, 1, 5);
}

BOOST_AUTO_TEST_CASE(Test_node_split_algorithm_22) {

    /* Split middle node in:
     *          [inner]
     *         /   |   \
     *  [leaf0] [leaf1] [leaf2] ... [leaf31]
     *
     * The result of the first split should look like this:
     *          [inner]
     *         /   |   \
     *  [leaf0] [inner] [leaf3] ... [leaf31]
     *           /   \
     *       [leaf1] [leaf2]
     *
     * The result of the first split should look like this:
     *          [inner]
     *         /   |   \
     *  [leaf0] [inner] [leaf4] ... [leaf34]
     *         /   |   \
     *  [leaf1] [leaf2] [leaf2]
     */

    std::map<int, std::vector<aku_Timestamp>> tss = {
        { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
        { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
        { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    };
    for (int i = 3; i < 32; i++) {
        tss[i] = { static_cast<aku_Timestamp>(i*10 + 1) };
    }
    test_node_split_algorithm_lvl2_split_twice(15, 17, tss, 2, 34);
}
