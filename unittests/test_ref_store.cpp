#include <iostream>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <boost/test/test_tools.hpp>
#include <vector>

#include "storage_engine/compression.h"
#include "storage_engine/ref_store.h"


using namespace Akumuli;
using namespace Akumuli::StorageEngine;

std::ostream& operator << (std::ostream& o, const SubtreeRef& ref) {
    o << "[fanout:" << ref.fanout_index << ",level" << ref.level << "]";
    return o;
}

bool operator == (const SubtreeRef& lhs, const SubtreeRef& rhs) {
    union {
        SubtreeRef fields;
        char binary[sizeof(SubtreeRef)];
    } ulhs, urhs;
    ulhs.fields = lhs;
    urhs.fields = rhs;
    return std::vector<char>(ulhs.binary, ulhs.binary + sizeof(SubtreeRef)) ==
           std::vector<char>(urhs.binary, urhs.binary + sizeof(SubtreeRef));
}

BOOST_AUTO_TEST_CASE(Test_encoding_size) {
    u8 buffer[0x1000];
    SubtreeRef ref;
    ref.addr = 0x11111;
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    ref.begin = basets * nano;
    ref.end   = (basets + 10) * nano;
    ref.count = 555;
    ref.checksum = 0;
    ref.fanout_index = 11;
    ref.first = 128.12849292;
    ref.id = 100000;
    ref.last = 223.93281818;
    ref.level = 2;
    ref.max = 3923.828282;
    ref.min = 82.82874928;
    ref.max_time = (basets + 2) * nano;
    ref.min_time = (basets + 4) * nano;
    ref.payload_size = 3982;
    ref.sum = 284272.192841;
    ref.type = NBTreeBlockType::INNER;
    ref.version = 1;
    auto out = SubtreeRefCompressor::encode_subtree_ref(buffer, 0x1000, ref);
    size_t outsz = out - buffer;
    BOOST_REQUIRE(outsz < sizeof(SubtreeRef));
}

static void requre_equal(SubtreeRef ref, SubtreeRef res) {
    BOOST_REQUIRE_EQUAL(ref.id, res.id);
    BOOST_REQUIRE_EQUAL(ref.version, res.version);
    BOOST_REQUIRE_EQUAL(ref.begin, res.begin);
    BOOST_REQUIRE_EQUAL(ref.end, res.end);
    BOOST_REQUIRE_EQUAL(ref.count, res.count);
    BOOST_REQUIRE_EQUAL(ref.checksum, res.checksum);
    BOOST_REQUIRE_EQUAL(ref.fanout_index, res.fanout_index);
    BOOST_REQUIRE_EQUAL(ref.first, res.first);
    BOOST_REQUIRE_EQUAL(ref.last, res.last);
    BOOST_REQUIRE_EQUAL(ref.level, res.level);
    BOOST_REQUIRE_EQUAL(ref.level, res.level);
    BOOST_REQUIRE_EQUAL(ref.max, res.max);
    BOOST_REQUIRE_EQUAL(ref.max_time, res.max_time);
    BOOST_REQUIRE_EQUAL(ref.min, res.min);
    BOOST_REQUIRE_EQUAL(ref.min_time, res.min_time);
    BOOST_REQUIRE_EQUAL(ref.payload_size, res.payload_size);
    BOOST_REQUIRE_EQUAL(ref.sum, res.sum);
    BOOST_REQUIRE_EQUAL(static_cast<u16>(ref.type), static_cast<u16>(res.type));
}

BOOST_AUTO_TEST_CASE(Test_roundtrip_inner_node) {
    u8 buffer[0x1000];
    SubtreeRef ref;
    ref.addr = 0x11111;
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    ref.begin = basets * nano;
    ref.end   = (basets + 60) * nano;
    ref.count = 1000;
    ref.checksum = 0;
    ref.fanout_index = 10;
    ref.first = 3.14159;
    ref.id = 100000;
    ref.last = 6.70318;
    ref.level = 2;
    ref.max = 92.112;
    ref.min = 2.113;
    ref.max_time = (basets + 10) * nano;
    ref.min_time = (basets + 20) * nano;
    ref.payload_size = 31;
    ref.sum = 284272.192841;
    ref.type = NBTreeBlockType::INNER;
    ref.version = 1;
    auto out1 = SubtreeRefCompressor::encode_subtree_ref(buffer, 0x1000, ref);

    SubtreeRef res;
    auto out2 = SubtreeRefCompressor::decode_subtree_ref(buffer, 0x1000, &res);

    // This fields are not stored
    res.id = ref.id;
    res.version = ref.version;

    requre_equal(ref, res);
    BOOST_REQUIRE_EQUAL(out1, out2);
}

BOOST_AUTO_TEST_CASE(Test_roundtrip_leaf_node) {
    u8 buffer[0x1000];
    SubtreeRef ref;
    ref.addr = 0x11111;
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    ref.begin = basets * nano;
    ref.end   = (basets + 60) * nano;
    ref.count = 1000;
    ref.checksum = 0;
    ref.fanout_index = 10;
    ref.first = 3.14159;
    ref.id = 100000;
    ref.last = 6.70318;
    ref.level = 1;
    ref.max = 92.112;
    ref.min = 2.113;
    ref.max_time = (basets + 10) * nano;
    ref.min_time = (basets + 20) * nano;
    ref.payload_size = 3998;
    ref.sum = 284272.192841;
    ref.type = NBTreeBlockType::LEAF;
    ref.version = 1;
    auto out1 = SubtreeRefCompressor::encode_subtree_ref(buffer, 0x1000, ref);

    SubtreeRef res;
    auto out2 = SubtreeRefCompressor::decode_subtree_ref(buffer, 0x1000, &res);

    // This fields are not stored
    res.id = ref.id;
    res.version = ref.version;

    requre_equal(ref, res);
    BOOST_REQUIRE_EQUAL(out1, out2);
}

BOOST_AUTO_TEST_CASE(Test_refstore_iter) {
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    SubtreeRef proto;
    proto.addr = 0x11111;
    proto.begin = basets * nano;
    proto.end   = (basets + 60) * nano;
    proto.count = 1000;
    proto.checksum = 0;
    proto.fanout_index = 0;
    proto.first = 3.14159;
    proto.id = 100000;
    proto.last = 6.70318;
    proto.level = 1;
    proto.max = 92.112;
    proto.min = 2.113;
    proto.max_time = (basets + 10) * nano;
    proto.min_time = (basets + 20) * nano;
    proto.payload_size = 3998;
    proto.sum = 284272.192841;
    proto.type = NBTreeBlockType::LEAF;
    proto.version = 1;

    CompressedRefStorage refstore(proto.id, proto.version);

    // Add a bunch of values with varying fanout
    for (u16 i = 0; i < 32; i++) {
        proto.fanout_index = i;
        refstore.append(proto);
    }

    u16 expected = 0;
    refstore.iter([&](const SubtreeRef& it) {
        proto.fanout_index = expected++;
        requre_equal(proto, it);
        return true;
    });

    BOOST_REQUIRE(expected == 32);
}


BOOST_AUTO_TEST_CASE(Test_refstore_remove_level) {
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    SubtreeRef proto;
    proto.addr = 0x11111;
    proto.begin = basets * nano;
    proto.end   = (basets + 60) * nano;
    proto.count = 1000;
    proto.checksum = 0;
    proto.fanout_index = 0;
    proto.first = 3.14159;
    proto.id = 100000;
    proto.last = 6.70318;
    proto.level = 0;
    proto.max = 92.112;
    proto.min = 2.113;
    proto.max_time = (basets + 10) * nano;
    proto.min_time = (basets + 20) * nano;
    proto.payload_size = 3998;
    proto.sum = 284272.192841;
    proto.type = NBTreeBlockType::LEAF;
    proto.version = 1;

    CompressedRefStorage refstore(proto.id, proto.version);

    // Add a bunch of values with varying fanout
    for (u16 i = 0; i < 32; i++) {
        proto.fanout_index = i;
        refstore.append(proto);
    }

    // Add next level
    proto.level = 1;
    proto.type = NBTreeBlockType::INNER;
    for (u16 i = 0; i < 32; i++) {
        proto.fanout_index = i;
        refstore.append(proto);
    }

    // Remove first level
    refstore.remove_level(0);

    u16 expected = 0;
    refstore.iter([&](const SubtreeRef& it) {
        proto.fanout_index = expected++;
        requre_equal(proto, it);
        return true;
    });

    BOOST_REQUIRE(expected == 32);
}

struct TreeMock {
    std::vector<SubtreeRef> refs_;

    size_t nelements() const {
        return refs_.size();
    }

    aku_Status append(const SubtreeRef& ref) {
        refs_.push_back(ref);
        return AKU_SUCCESS;
    }

    aku_Status read_all(std::vector<SubtreeRef>* refs) const {
        std::copy(refs_.begin(), refs_.end(), std::back_inserter(*refs));
        return AKU_SUCCESS;
    }
};


BOOST_AUTO_TEST_CASE(Test_refstore_load_store) {
    auto basets = 1530291866ul;
    auto nano = 1000000ul;
    SubtreeRef proto;
    proto.addr = 0x11111;
    proto.begin = basets * nano;
    proto.end   = (basets + 60) * nano;
    proto.count = 1000;
    proto.checksum = 0;
    proto.fanout_index = 0;
    proto.first = 3.14159;
    proto.id = 100000;
    proto.last = 6.70318;
    proto.level = 0;
    proto.max = 92.112;
    proto.min = 2.113;
    proto.max_time = (basets + 10) * nano;
    proto.min_time = (basets + 20) * nano;
    proto.payload_size = 3998;
    proto.sum = 284272.192841;
    proto.type = NBTreeBlockType::LEAF;
    proto.version = 1;


    std::vector<SubtreeRef> refs;
    // Add a bunch of values with varying fanout
    for (u16 i = 0; i < 32; i++) {
        proto.fanout_index = i;
        refs.push_back(proto);
    }

    CompressedRefStorage refstore(proto.id, proto.version);

    TreeMock mock;
    mock.refs_ = refs;

    auto status = refstore.loadFrom(mock);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    u16 expected = 0;
    refstore.iter([&](const SubtreeRef& it) {
        proto.fanout_index = expected++;
        requre_equal(proto, it);
        return true;
    });
    BOOST_REQUIRE(expected == 32);

    TreeMock refsout;
    status = refstore.saveTo(&refsout);
    BOOST_REQUIRE_EQUAL(status, AKU_SUCCESS);

    BOOST_REQUIRE_EQUAL(refs.size(), refsout.refs_.size());
    for (size_t i = 0; i < refs.size(); i++) {
        BOOST_REQUIRE(refs.at(i) == refsout.refs_.at(i));
    }
}
