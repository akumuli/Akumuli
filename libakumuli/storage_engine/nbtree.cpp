// C++
#include <algorithm>

// App
#include "nbtree.h"
#include "akumuli_version.h"


namespace Akumuli {
namespace StorageEngine {

//! This value represents empty addr. It's too large to be used as a real block addr.
static const LogicAddr EMPTY = std::numeric_limits<LogicAddr>::max();

static SubtreeRef* subtree_cast(uint8_t* p) {
    return reinterpret_cast<SubtreeRef*>(p);
}

static SubtreeRef const* subtree_cast(uint8_t const* p) {
    return reinterpret_cast<SubtreeRef const*>(p);
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
    SubtreeRef const* subtree = subtree_cast(buffer_.data());
    return std::make_tuple(subtree->begin, subtree->end);
}

LogicAddr NBTreeLeaf::get_prev_addr() const {
    // Should be set correctly no metter how NBTreeLeaf was created.
    return prev_;
}


aku_Status NBTreeLeaf::read_all(std::vector<aku_Timestamp>* timestamps,
                                std::vector<double>* values) const
{
    if (buffer_.size() == sizeof(SubtreeRef)) {
        // Error. Page is not fully loaded
        return AKU_ENO_DATA;
    }
    int windex = writer_.get_write_index();
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
    // Read tail elements from `writer_`
    if (windex != 0) {
        writer_.read_tail_elements(timestamps, values);
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
//     NBTreeSuperblock     //
// //////////////////////// //

NBTreeSuperblock::NBTreeSuperblock(aku_ParamId id)
    : buffer_(AKU_BLOCK_SIZE, 0)
    , id_(id)
{
}

aku_Status NBTreeSuperblock::append(LogicAddr addr, const NBTreeLeaf& leaf) {
    SubtreeRef ref;
    ref.addr = addr;
    // calculate leaf stats
    std::vector<aku_Timestamp> ts;
    std::vector<double> xs;
    aku_Status status = leaf.read_all(&ts, &xs);
    if (status != AKU_SUCCESS) {
        return status;
    }
    if (xs.empty()) {
        // Can't add empty leaf node to the node!
        return AKU_ENO_DATA;
    }
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    double sum = 0;
    for (auto x: xs) {
        min = std::min(min, x);
        max = std::max(max, x);
        sum = sum + x;
    }
    ref.max = max;
    ref.min = min;
    ref.sum = sum;
    ref.begin = ts.front();
    ref.end = ts.back();
    ref.level = 1;  // because leaf node was added
    ref.version = AKUMULI_VERSION;
    ref.payload_size = 0;  // Not used in supernodes
    ref.id = id_;
    ref.count = (uint32_t)xs.size();
    AKU_UNUSED(ref); // TODO: write it into buffer
    return AKU_SUCCESS;
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
    leaf_.reset(new NBTreeLeaf(id_, last_));
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

aku_Status NBTree::read_all(std::vector<aku_Timestamp>* timestamps, std::vector<double>* values) const {
    return leaf_->read_all(timestamps, values);
}

std::vector<LogicAddr> NBTree::iter(aku_Timestamp start, aku_Timestamp stop) const {
    // Traverse tree from largest timestamp to smallest
    aku_Timestamp min = std::min(start, stop);
    aku_Timestamp max = std::max(start, stop);
    LogicAddr addr = last_;
    std::vector<LogicAddr> addresses;
    // Stop when EMPTY is hit or cycle detected.
    while (bstore_->exists(addr)) {
        std::unique_ptr<NBTreeLeaf> leaf;
        leaf.reset(new NBTreeLeaf(bstore_, addr, NBTreeLeaf::LeafLoadMethod::ONLY_HEADER));
        aku_Timestamp begin, end;
        std::tie(begin, end) = leaf->get_timestamps();
        if (min > end || max < begin) {
            addr = leaf->get_prev_addr();
            continue;
        }
        // Save address of the current leaf and move to the next one.
        addresses.push_back(addr);
        addr = leaf->get_prev_addr();
    }
    return addresses;
}

std::unique_ptr<NBTreeLeaf> NBTree::load(LogicAddr addr) const {
    std::unique_ptr<NBTreeLeaf> leaf;
    leaf.reset(new NBTreeLeaf(bstore_, addr));
    return std::move(leaf);
}

NBTreeCursor::NBTreeCursor(NBTree const& tree, aku_Timestamp start, aku_Timestamp stop)
    : tree_(tree)
    , start_(start)
    , stop_(stop)
    , eof_(false)
    , proceed_calls_(0)
    , id_(tree.get_id())
{
    ts_.reserve(SPACE_RESERVE);
    value_.reserve(SPACE_RESERVE);
    auto addrlist = tree.iter(start, stop);
    if (start > stop) {
        // Forward direction. Method tree.iter always return path in
        // backward direction so we need to reverse it.
        std::reverse(addrlist.begin(), addrlist.end());
    }
    std::swap(addrlist, backpath_);
    proceed();
}

aku_Status NBTreeCursor::load_next_page() {
    if (backpath_.empty()) {
        return AKU_ENO_DATA;
    }
    LogicAddr addr = backpath_.back();
    backpath_.pop_back();
    std::unique_ptr<NBTreeLeaf> leaf = tree_.load(addr);
    ts_.clear();
    value_.clear();
    return leaf->read_all(&ts_, &value_);
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

void NBTreeCursor::proceed() {
    aku_Status status = AKU_SUCCESS;
    if (start_ > stop_) {
        // Forward direction
        if (proceed_calls_ == 0) {
            // First call to proceed, need to push data from
            // tree_ first.
            ts_.clear();
            value_.clear();
            status = tree_.read_all(&ts_, &value_);
            // Ignore ENO_DATA error
            if (status != AKU_ENO_DATA && status != AKU_SUCCESS) {
                AKU_PANIC("Page load error");  // TODO: error translation
            }
            proceed_calls_++;
            return;
        }
    } else {
        // If backpath_ is empty we need to read data from tree_.
        if (backpath_.empty() && proceed_calls_ >= 0) {
            ts_.clear();
            value_.clear();
            status = tree_.read_all(&ts_, &value_);
            if (status != AKU_ENO_DATA && status != AKU_SUCCESS) {
                AKU_PANIC("Page load error");  // TODO: error translation
            }
            proceed_calls_ = -1;
            return;
        } else if (backpath_.empty() && proceed_calls_ < 0) {
            eof_ = true;
            return;
        }
    }
    status = load_next_page();
    if (status == AKU_ENO_DATA) {
        eof_ = true;
    } else if (status != AKU_SUCCESS) {
        AKU_PANIC("Page load error");  // TODO: translate error message from `status`
    }
    proceed_calls_++;
}

}}
