// C++
#include <algorithm>

// App
#include "nbtree.h"
#include "akumuli_version.h"


namespace Akumuli {
namespace StorageEngine {

//! This value represents empty addr. It's too large to be used as a real block addr.
static const LogicAddr EMPTY = std::numeric_limits<LogicAddr>::max();

/** Reference to tree node.
  * Ref contains some metadata: version, level, payload_size, id.
  * This metadata corresponds to the current node.
  * Also, reference contains some aggregates: count, begin, end, min, max, sum.
  * This aggregates corresponds to the current node if leve=0 (current node is a
  * leaf node) or to the pointee if level > 0. If level is 1 then pointee is a
  * leafa node and all this fields describes this leaf node. If level is 2 or more
  * then all this aggregates comes from entire subtree (e.g. min is a minimal value
  * in leaf nodes in pointee subtree).
  */
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

NBTreeLeaf::NBTreeLeaf(aku_ParamId id, LogicAddr prev)
    : prev_(prev)
    , buffer_(AKU_BLOCK_SIZE, 0)
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

NBTreeLeaf::NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr, LeafLoadMethod load)
    : prev_(EMPTY)
{
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can't read block");  // TODO: translate status to error code
    }
    if (load == LeafLoadMethod::FULL_PAGE_LOAD) {
        buffer_.reserve(block->get_size());
        std::copy(block->get_data(), block->get_data() + block->get_size(),
                  std::back_inserter(buffer_));
    } else {
        buffer_.reserve(sizeof(SubtreeRef));
        std::copy(block->get_data(), block->get_data() + sizeof(SubtreeRef),
                  std::back_inserter(buffer_));
    }
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    prev_ = subtree->addr;
}

size_t NBTreeLeaf::nelements() {
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    return subtree->count;
}


std::tuple<aku_Timestamp, aku_Timestamp> NBTreeLeaf::get_timestamps() const {
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    return std::make_tuple(subtree->begin, subtree->end);
}

LogicAddr NBTreeLeaf::get_prev_addr() const {
    // Should be set correctly no metter how NBTreeLeaf was created.
    return prev_;
}


aku_Status NBTreeLeaf::read_all(std::vector<aku_Timestamp>* timestamps, std::vector<double>* values) {
    if (buffer_.size() == sizeof(SubtreeRef)) {
        // Error. Page is not fully loaded
        return AKU_ENO_DATA;
    }
    DataBlockReader reader(buffer_.data() + sizeof(SubtreeRef), buffer_.size());
    size_t sz = reader.nelements();
    timestamps->reserve(sz);
    values->reserve(sz);
    for (size_t ix = 0; ix < sz; ix++) {
        aku_Status status;
        aku_Timestamp ts;
        double value;
        std::tie(status, ts, value) = reader.next();
        if (status != AKU_SUCCESS) {
            return status;
        }
        timestamps->push_back(ts);
        values->push_back(value);
    }
    return AKU_SUCCESS;
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

std::tuple<aku_Status, LogicAddr> NBTreeLeaf::commit(std::shared_ptr<BlockStore> bstore) {
    size_t size = writer_.commit();
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    subtree->payload_size = size;
    return bstore->append_block(buffer_.data());

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
        std::tie(status, addr) = leaf_->commit(bstore_);
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

aku_ParamId NBTree::get_id() const {
    return id_;
}

std::vector<LogicAddr> NBTree::iter(aku_Timestamp start, aku_Timestamp stop) const {
    // Traverse tree from largest timestamp to smallest
    aku_Timestamp min = std::min(start, stop);
    aku_Timestamp max = std::max(start, stop);
    LogicAddr addr = last_;
    std::vector<LogicAddr> addresses;
    while (addr != EMPTY) {
        std::unique_ptr<NBTreeLeaf> leaf;
        leaf.reset(new NBTreeLeaf(bstore_, last_, NBTreeLeaf::LeafLoadMethod::ONLY_HEADER));
        aku_Timestamp begin, end;
        std::tie(begin, end) = leaf->get_timestamps();
        if (min > end || max < begin) {
            addr = leaf->get_prev_addr();
            continue;
        }
        // Save address of the current leaf and move to the next one
        addresses.push_back(addr);
        addr = leaf->get_prev_addr();
    }
    return addresses;
}

NBTreeCursor::NBTreeCursor(NBTree const& tree, aku_Timestamp start, aku_Timestamp stop)
    : tree_(tree)
    , start_(start)
    , stop_(stop)
    , eof_(false)
    , id_(tree.get_id())
{
    ts_.reserve(SPACE_RESERVE);
    value_.reserve(SPACE_RESERVE);
    auto addrlist = tree.iter(start, stop);
    if (start < stop) {
        // Forward direction. Method tree.iter always return path in
        // backward direction so we need to reverse it.
        std::reverse(addrlist.begin(), addrlist.end());
    }
    std::swap(addrlist, backpath_);

    // TODO: load first page from backpath_ into ts_ and value_.
}

//! Returns number of elements in cursor
size_t NBTreeCursor::size() {
    return ts_.size();
}

//! Return true if read operation is completed and elements stored in this cursor
//! are the last ones.
bool NBTreeCursor::is_eof() {
    return eof_;
}

//! Read element from cursor (not all elements can be loaded to cursor)
std::tuple<aku_Status, aku_Timestamp, double> NBTreeCursor::at(size_t ix) {
    if (ix < ts_.size()) {
        return std::make_tuple(AKU_SUCCESS, ts_[ix], value_[ix]);
    }
    return std::make_tuple(AKU_EBAD_ARG, 0, 0);  // Index out of range
}

void NBtreeCursor::proceed_next() {
    throw "not implemented";
}

}}
