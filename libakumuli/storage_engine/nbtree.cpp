#include "nbtree.h"
#include "akumuli_version.h"

namespace Akumuli {
namespace StorageEngine {

//! This value represents empty addr. It's too large to be used as a real block addr.
static const LogicAddr EMPTY = std::numeric_limits<LogicAddr>::max();

//! Reference to tree node
struct SubtreeRef {
    //! Node version
    uint16_t      version;
    //! Node level in the tree
    uint16_t      level;
    //! Number of elements in the subtree
    uint32_t      count;
    //! Payload size (real)
    uint32_t      payload_size;
    //! Series Id
    aku_ParamId   id;
    //! First element's timestamp
    aku_Timestamp begin;
    //! Last element's timestamp
    aku_Timestamp end;
    //! Object addr in blockstore
    LogicAddr     addr;
    //! Smalles value
    double        min;
    //! Largest value
    double        max;
    //! Summ of all elements in subtree
    double        sum;
} __attribute__((packed));

static SubtreeRef* subtree_cast(uint8_t* p) {
    return reinterpret_cast<SubtreeRef*>(p);
}

NBTreeLeaf::NBTreeLeaf(aku_ParamId id, std::shared_ptr<BlockStore> bstore, LogicAddr prev)
    : bstore_(bstore)
    , prev_(prev)
    , buffer_(BLOCK_SIZE, 0)
    , writer_(id, buffer_.data() + sizeof(SubtreeRef), AKU_BLOCK_SIZE - sizeof(SubtreeRef))
{
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    subtree->addr = prev;
    subtree->level = 0;  // Leaf node
    subtree->id = id;
    subtree->version = AKUMULI_VERSION;
    subtree->payload_size = 0;
    // values that should be updated by insert
    subtree->begin = std::numeric_limits<aku_Timestamp>::max();
    subtree->end = 0;
    subtree->count = 0;
    subtree->min = std::numeric_limits<double>::max();
    subtree->max = std::numeric_limits<double>::min();
    subtree->sum = 0;
}

aku_Status NBTreeLeaf::append(aku_Timestamp ts, double value) {
    aku_Status status = writer_.put(ts, value);
    if (status == AKU_SUCCESS) {
        SubtreeRef* subtree = subtree_cast(buffer_.data());
        subtree->end = ts;
        if (subtree->count == 0) {
            subtree->begin = ts;
        }
        subtree->count++;
        subtree->sum += value;
        subtree->max = std::max(subtree->max, value);
        subtree->min = std::min(subtree->min, value);
    }
    return status;
}

std::tuple<aku_Status, LogicAddr> NBTreeLeaf::commit() {
    size_t size = writer_.commit();
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    subtree->payload_size = size;
    return bstore_->append_block(buffer_.data());

}

// //////////////////////// //
//          NBTree          //
// //////////////////////// //


NBTree::NBTree(aku_ParamId id, std::shared_ptr<BlockStore> bstore)
    : bstore_(bstore)
    , id_(id)
    , last_(EMPTY)
{
    reset_leaf();
}

void NBTree::reset_leaf() {
    leaf_.reset(new NBTreeLeaf(id_, bstore_, last_));
}

void NBTree::append(aku_Timestamp ts, double value) {
    // Invariant: leaf_ should be initialized, if leaf_ is full and pushed to block-store, reset_leaf should be called
    aku_Status status = leaf_->append(ts, value);
    if (status == AKU_EOVERFLOW) {
        LogicAddr addr;
        std::tie(status, addr) = leaf_->commit();
        if (status != AKU_SUCCESS) {
            AKU_PANIC("Can't append data to the NBTree instance");
        }
        last_ = addr;
        reset_leaf();
        // There should be only one level of recursion, no looping.
        // Stack overflow here means that there is a logic error in
        // the program that results in NBTreeLeaf::append always
        // returning AKU_EOVERFLOW.
        append(ts, value);
    }
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Unexpected error from NBTreeLeaf");  // it should return only AKU_EOVERFLOW
    }
}

std::vector<LogicAddr> NBTree::roots() const {
    // NOTE: at this development stage implementation tracks only leaf nodes.
    // This is enough to test performance and build server and ingestion.
    std::vector<LogicAddr> rv = { last_ };
    return rv;
}

}}
