/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// C++
#include <iostream>  // For debug print fn.
#include <algorithm>
#include <vector>
#include <sstream>
#include <stack>
#include <array>

// App
#include "nbtree.h"
#include "nbtree_iter.h"
#include "akumuli_version.h"
#include "status_util.h"
#include "log_iface.h"
#include "operators/scan.h"
#include "operators/aggregate.h"


namespace Akumuli {
namespace StorageEngine {

std::ostream& operator << (std::ostream& out, NBTreeBlockType blocktype) {
    if (blocktype == NBTreeBlockType::LEAF) {
        out << "NBTreeLeaf";
    } else {
        out << "NBTreeSuperblock";
    }
    return out;
}


static std::string to_string(const SubtreeRef& ref) {
    std::stringstream fmt;
    fmt                     << "{"
        << ref.count        << ", "
        << ref.id           << ", "
        << ref.begin        << ", "
        << ref.end          << ", "
        << ref.addr         << ", "
        << ref.min          << ", "
        << ref.min_time     << ", "
        << ref.max          << ", "
        << ref.max_time     << ", "
        << ref.sum          << ", "
        << ref.first        << ", "
        << ref.last         << ", "
        << ref.type         << ", "
        << ref.level        << ", "
        << ref.payload_size << ", "
        << ref.version      << ", "
        << ref.fanout_index << ", "
        << ref.checksum     << "}";
    return fmt.str();
}

//! Read block from blockstoroe with all the checks. Panic on error!
static std::shared_ptr<Block> read_block_from_bstore(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read block @" + std::to_string(curr) + ", error: " + StatusUtil::str(status));
        AKU_PANIC("Can't read block - " + StatusUtil::str(status));
    }
    // Check consistency (works with both inner and leaf nodes).
    u8 const* data = block->get_cdata();
    SubtreeRef const* subtree = subtree_cast(data);
    u32 crc = bstore->checksum(data + sizeof(SubtreeRef), subtree->payload_size);
    if (crc != subtree->checksum) {
        std::stringstream fmt;
        fmt << "Invalid checksum (addr: " << curr << ", level: " << subtree->level << ")";
        AKU_PANIC(fmt.str());
    }
    return block;
}

//! Read block from blockstoroe with all the checks. Panic on error!
static std::shared_ptr<IOVecBlock> read_iovec_block_from_bstore(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<IOVecBlock> block;
    std::tie(status, block) = bstore->read_iovec_block(curr);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read block @" + std::to_string(curr) + ", error: " + StatusUtil::str(status));
        AKU_PANIC("Can't read block - " + StatusUtil::str(status));
    }
    // Check consistency (works with both inner and leaf nodes).
    u8 const* data = block->get_cdata(0);
    SubtreeRef const* subtree = subtree_cast(data);
    u32 crc = bstore->checksum(data + sizeof(SubtreeRef), subtree->payload_size);
    if (crc != subtree->checksum) {
        std::stringstream fmt;
        fmt << "Invalid checksum (addr: " << curr << ", level: " << subtree->level << ")";
        AKU_PANIC(fmt.str());
    }
    return block;
}

//! Initialize object from leaf node
aku_Status init_subtree_from_leaf(const NBTreeLeaf& leaf, SubtreeRef& out) {
    if (leaf.nelements() == 0) {
        return AKU_EBAD_ARG;
    }
    SubtreeRef const* meta = leaf.get_leafmeta();
    out = *meta;
    out.payload_size = 0;
    out.checksum = 0;
    out.addr = EMPTY_ADDR;  // Leaf metadta stores address of the previous node!
    out.type = NBTreeBlockType::LEAF;
    return AKU_SUCCESS;
}

//! Initialize object from leaf node
static aku_Status init_subtree_from_leaf(const IOVecLeaf& leaf, SubtreeRef& out) {
    if (leaf.nelements() == 0) {
        return AKU_EBAD_ARG;
    }
    SubtreeRef const* meta = leaf.get_leafmeta();
    out = *meta;
    out.payload_size = 0;
    out.checksum = 0;
    out.addr = EMPTY_ADDR;  // Leaf metadta stores address of the previous node!
    out.type = NBTreeBlockType::LEAF;
    return AKU_SUCCESS;
}

aku_Status init_subtree_from_subtree(const NBTreeSuperblock& node, SubtreeRef& backref) {
    std::vector<SubtreeRef> refs;
    aku_Status status = node.read_all(&refs);
    if (status != AKU_SUCCESS) {
        return status;
    }
    backref.begin = refs.front().begin;
    backref.end = refs.back().end;
    backref.first = refs.front().first;
    backref.last = refs.back().last;
    backref.count = 0;
    backref.sum = 0;

    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    aku_Timestamp mints = 0;
    aku_Timestamp maxts = 0;
    for (const SubtreeRef& sref: refs) {
        backref.count += sref.count;
        backref.sum   += sref.sum;
        if (min > sref.min) {
            min = sref.min;
            mints = sref.min_time;
        }
        if (max < sref.max) {
            max = sref.max;
            maxts = sref.max_time;
        }
    }
    backref.min = min;
    backref.max = max;
    backref.min_time = mints;
    backref.max_time = maxts;

    // Node level information
    backref.id = node.get_id();
    backref.level = node.get_level();
    backref.type = NBTreeBlockType::INNER;
    backref.version = AKUMULI_VERSION;
    backref.fanout_index = node.get_fanout();
    backref.payload_size = 0;
    return AKU_SUCCESS;
}

// //////////////// //
//    NBTreeLeaf    //
// //////////////// //

NBTreeLeaf::NBTreeLeaf(aku_ParamId id, LogicAddr prev, u16 fanout_index)
    : prev_(prev)
    , block_(std::make_shared<Block>())
    , writer_(id, block_->get_data() + sizeof(SubtreeRef), AKU_BLOCK_SIZE - sizeof(SubtreeRef))
    , fanout_index_(fanout_index)
{
    // Check that invariant holds.
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    subtree->addr = prev;
    subtree->level = 0;  // Leaf node
    subtree->type = NBTreeBlockType::LEAF;
    subtree->id = id;
    subtree->version = AKUMULI_VERSION;
    subtree->payload_size = 0;
    subtree->fanout_index = fanout_index;
    // values that should be updated by insert
    subtree->begin = std::numeric_limits<aku_Timestamp>::max();
    subtree->end = 0;
    subtree->count = 0;
    subtree->min = std::numeric_limits<double>::max();
    subtree->max = std::numeric_limits<double>::min();
    subtree->sum = 0;
    subtree->min_time = std::numeric_limits<aku_Timestamp>::max();
    subtree->max_time = std::numeric_limits<aku_Timestamp>::min();
    subtree->first = .0;
    subtree->last = .0;
}


NBTreeLeaf::NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr)
    : NBTreeLeaf(read_block_from_bstore(bstore, curr))
{
}

NBTreeLeaf::NBTreeLeaf(std::shared_ptr<Block> block)
    : prev_(EMPTY_ADDR)
{
    block_ = block;
    const SubtreeRef* subtree = subtree_cast(block_->get_cdata());
    prev_ = subtree->addr;
    fanout_index_ = subtree->fanout_index;
}

static std::shared_ptr<Block> clone(std::shared_ptr<Block> block) {
    auto res = std::make_shared<Block>();
    memcpy(res->get_data(), block->get_cdata(), AKU_BLOCK_SIZE);
    return res;
}

static aku_ParamId getid(std::shared_ptr<Block> const& block) {
    auto ptr = reinterpret_cast<SubtreeRef const*>(block->get_cdata());
    return ptr->id;
}

NBTreeLeaf::NBTreeLeaf(std::shared_ptr<Block> block, NBTreeLeaf::CloneTag)
    : prev_(EMPTY_ADDR)
    , block_(clone(block))
    , writer_(getid(block_), block_->get_data() + sizeof(SubtreeRef), AKU_BLOCK_SIZE - sizeof(SubtreeRef))
{
    // Re-insert the data
    DataBlockReader reader(block->get_cdata() + sizeof(SubtreeRef), block->get_size());
    size_t sz = reader.nelements();
    for (size_t ix = 0; ix < sz; ix++) {
        aku_Status status;
        aku_Timestamp ts;
        double value;
        std::tie(status, ts, value) = reader.next();
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Leaf node clone error, can't read the previous node (some data will be lost)");
            assert(false);
            return;
        }
        status = writer_.put(ts, value);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Leaf node clone error, can't write to the new node (some data will be lost)");
            assert(false);
            return;
        }
    }

    const SubtreeRef* subtree = subtree_cast(block_->get_cdata());
    prev_ = subtree->addr;
    fanout_index_ = subtree->fanout_index;
}

size_t NBTreeLeaf::_get_uncommitted_size() const {
    return static_cast<size_t>(writer_.get_write_index());
}

SubtreeRef const* NBTreeLeaf::get_leafmeta() const {
    return subtree_cast(block_->get_cdata());
}

size_t NBTreeLeaf::nelements() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata());
    return subtree->count;
}

u16 NBTreeLeaf::get_fanout() const {
    return fanout_index_;
}

aku_ParamId NBTreeLeaf::get_id() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata());
    return subtree->id;
}

std::tuple<aku_Timestamp, aku_Timestamp> NBTreeLeaf::get_timestamps() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata());
    return std::make_tuple(subtree->begin, subtree->end);
}

void NBTreeLeaf::set_prev_addr(LogicAddr addr) {
    prev_ = addr;
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    subtree->addr = addr;
}

void NBTreeLeaf::set_node_fanout(u16 fanout) {
    assert(fanout <= AKU_NBTREE_FANOUT);
    fanout_index_ = fanout;
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    subtree->fanout_index = fanout;
}

LogicAddr NBTreeLeaf::get_addr() const {
    return block_->get_addr();
}

LogicAddr NBTreeLeaf::get_prev_addr() const {
    // Should be set correctly no metter how NBTreeLeaf was created.
    return prev_;
}


aku_Status NBTreeLeaf::read_all(std::vector<aku_Timestamp>* timestamps,
                                std::vector<double>* values) const
{
    int windex = writer_.get_write_index();
    DataBlockReader reader(block_->get_cdata() + sizeof(SubtreeRef), block_->get_size());
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
        SubtreeRef* subtree = subtree_cast(block_->get_data());
        subtree->end = ts;
        subtree->last = value;
        if (subtree->count == 0) {
            subtree->begin = ts;
            subtree->first = value;
        }
        subtree->count++;
        subtree->sum += value;
        if (subtree->max < value) {
            subtree->max = value;
            subtree->max_time = ts;
        }
        if (subtree->min > value) {
            subtree->min = value;
            subtree->min_time = ts;
        }
    }
    return status;
}

std::tuple<aku_Status, LogicAddr> NBTreeLeaf::commit(std::shared_ptr<BlockStore> bstore) {
    assert(nelements() != 0);
    u16 size = static_cast<u16>(writer_.commit());
    assert(size);
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    subtree->payload_size = size;
    if (prev_ != EMPTY_ADDR && fanout_index_ > 0) {
        subtree->addr = prev_;
    } else {
        // addr = EMPTY indicates that there is
        // no link to previous node.
        subtree->addr  = EMPTY_ADDR;
        // Invariant: fanout index should be 0 in this case.
    }
    subtree->version = AKUMULI_VERSION;
    subtree->level = 0;
    subtree->type  = NBTreeBlockType::LEAF;
    subtree->fanout_index = fanout_index_;
    // Compute checksum
    subtree->checksum = bstore->checksum(block_->get_cdata() + sizeof(SubtreeRef), size);
    return bstore->append_block(block_);
}


std::unique_ptr<RealValuedOperator> NBTreeLeaf::range(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<RealValuedOperator> it;
    it.reset(new NBTreeLeafIterator(begin, end, *this));
    return it;
}

std::unique_ptr<RealValuedOperator> NBTreeLeaf::filter(aku_Timestamp begin,
                                                       aku_Timestamp end,
                                                       const ValueFilter& filter) const
{
    std::unique_ptr<RealValuedOperator> it;
    it.reset(new NBTreeLeafFilter(begin, end, filter, *this));
    return it;
}

std::unique_ptr<AggregateOperator> NBTreeLeaf::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<AggregateOperator> it;
    it.reset(new NBTreeLeafAggregator(begin, end, *this));
    return it;
}

std::unique_ptr<AggregateOperator> NBTreeLeaf::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    AKU_UNUSED(hint);
    auto agg = INIT_AGGRES;
    const SubtreeRef* subtree = subtree_cast(block_->get_cdata());
    agg.copy_from(*subtree);
    std::unique_ptr<AggregateOperator> result;
    AggregateOperator::Direction dir = begin < end ? AggregateOperator::Direction::FORWARD : AggregateOperator::Direction::BACKWARD;
    result.reset(new ValueAggregator(subtree->end, agg, dir));
    return result;
}

std::unique_ptr<AggregateOperator> NBTreeLeaf::group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const {
    std::unique_ptr<AggregateOperator> it;
    it.reset(new NBTreeLeafGroupAggregator(begin, end, step, *this));
    return it;
}

std::tuple<aku_Status, LogicAddr> NBTreeLeaf::split_into(std::shared_ptr<BlockStore> bstore,
                                                         aku_Timestamp pivot,
                                                         bool preserve_backrefs,
                                                         u16 *fanout_index,
                                                         NBTreeSuperblock* top_level)
{
    /* When the method is called from NBTreeSuperblock::split method, the
     * top_level node will be provided. Otherwise it will be null.
     */
    aku_Status status;
    std::vector<double> xss;
    std::vector<aku_Timestamp> tss;
    status = read_all(&tss, &xss);
    if (status != AKU_SUCCESS || tss.size() == 0) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    // Make new superblock with two leafs
    // Left hand side leaf node
    u32 ixbase = 0;
    NBTreeLeaf lhs(get_id(), preserve_backrefs ? prev_ : EMPTY_ADDR, *fanout_index);
    for (u32 i = 0; i < tss.size(); i++) {
        if (tss[i] < pivot) {
            status = lhs.append(tss[i], xss[i]);
            if (status != AKU_SUCCESS) {
                return std::make_tuple(status, EMPTY_ADDR);
            }
        } else {
            ixbase = i;
            break;
        }
    }
    SubtreeRef lhs_ref;
    if (ixbase == 0) {
        // Special case, the lhs node is empty
        lhs_ref.addr = EMPTY_ADDR;
    } else {
        LogicAddr lhs_addr;
        std::tie(status, lhs_addr) = lhs.commit(bstore);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        status = init_subtree_from_leaf(lhs, lhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        lhs_ref.addr = lhs_addr;
        (*fanout_index)++;
    }
    // Right hand side leaf node, it can't be empty in any case
    // because the leaf node is not empty.
    auto prev = lhs_ref.addr == EMPTY_ADDR ? prev_ : lhs_ref.addr;
    NBTreeLeaf rhs(get_id(), prev, *fanout_index);
    for (u32 i = ixbase; i < tss.size(); i++) {
        status = rhs.append(tss[i], xss[i]);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    SubtreeRef rhs_ref;
    if (ixbase == tss.size()) {
        // Special case, rhs is empty
        rhs_ref.addr = EMPTY_ADDR;
    } else {
        LogicAddr rhs_addr;
        std::tie(status, rhs_addr) = rhs.commit(bstore);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        status = init_subtree_from_leaf(rhs, rhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        rhs_ref.addr = rhs_addr;
        (*fanout_index)++;
    }
    // Superblock
    if (lhs_ref.addr != EMPTY_ADDR) {
        status = top_level->append(lhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    if (rhs_ref.addr != EMPTY_ADDR) {
        status = top_level->append(rhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    return std::make_tuple(AKU_SUCCESS, EMPTY_ADDR);
}

std::tuple<aku_Status, LogicAddr> NBTreeLeaf::split(std::shared_ptr<BlockStore> bstore,
                                                    aku_Timestamp pivot,
                                                    bool preserve_backrefs)
{
    // New superblock
    NBTreeSuperblock sblock(get_id(), preserve_backrefs ? get_prev_addr() : EMPTY_ADDR, get_fanout(), 0);
    aku_Status status;
    LogicAddr  addr;
    u16 fanout = 0;
    std::tie(status, addr) = split_into(bstore, pivot, false, &fanout, &sblock);
    if (status != AKU_SUCCESS || sblock.nelements() == 0) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    std::tie(status, addr) = sblock.commit(bstore);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    return std::make_tuple(AKU_SUCCESS, addr);
}

// ///////// //
// IOVecLeaf //

IOVecLeaf::IOVecLeaf(aku_ParamId id, LogicAddr prev, u16 fanout_index)
    : prev_(prev)
    , block_(std::make_shared<IOVecBlock>())
    , writer_(block_.get())
    , fanout_index_(fanout_index)
{
    // Check that invariant holds.
    SubtreeRef* subtree = block_->allocate<SubtreeRef>();
    if (subtree == nullptr) {
        AKU_PANIC("Can't allocate space in IOVecBlock");
    }
    subtree->addr = prev;
    subtree->level = 0;  // Leaf node
    subtree->type = NBTreeBlockType::LEAF;
    subtree->id = id;
    subtree->version = AKUMULI_VERSION;
    subtree->payload_size = 0;
    subtree->fanout_index = fanout_index;
    // values that should be updated by insert
    subtree->begin = std::numeric_limits<aku_Timestamp>::max();
    subtree->end = 0;
    subtree->count = 0;
    subtree->min = std::numeric_limits<double>::max();
    subtree->max = std::numeric_limits<double>::min();
    subtree->sum = 0;
    subtree->min_time = std::numeric_limits<aku_Timestamp>::max();
    subtree->max_time = std::numeric_limits<aku_Timestamp>::min();
    subtree->first = .0;
    subtree->last = .0;

    // Initialize the writer
    writer_.init(id);
}


IOVecLeaf::IOVecLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr)
    : IOVecLeaf(read_iovec_block_from_bstore(bstore, curr))
{
}

IOVecLeaf::IOVecLeaf(std::shared_ptr<IOVecBlock> block)
    : prev_(EMPTY_ADDR)
    , block_(block)
{
    const SubtreeRef* subtree = subtree_cast(block_->get_cdata(0));
    prev_ = subtree->addr;
    fanout_index_ = subtree->fanout_index;
}

size_t IOVecLeaf::_get_uncommitted_size() const {
    return static_cast<size_t>(writer_.get_write_index());
}

size_t IOVecLeaf::bytes_used() const {
    size_t res = 0;
    for (int i = 0; i < IOVecBlock::NCOMPONENTS; i++) {
        res += block_->get_size(i);
    }
    return res;
}

SubtreeRef const* IOVecLeaf::get_leafmeta() const {
    return subtree_cast(block_->get_cdata(0));
}

size_t IOVecLeaf::nelements() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata(0));
    return subtree->count;
}

u16 IOVecLeaf::get_fanout() const {
    return fanout_index_;
}

aku_ParamId IOVecLeaf::get_id() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata(0));
    return subtree->id;
}

std::tuple<aku_Timestamp, aku_Timestamp> IOVecLeaf::get_timestamps() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_cdata(0));
    return std::make_tuple(subtree->begin, subtree->end);
}

void IOVecLeaf::set_prev_addr(LogicAddr addr) {
    prev_ = addr;
    SubtreeRef* subtree = subtree_cast(block_->get_data(0));
    subtree->addr = addr;
}

void IOVecLeaf::set_node_fanout(u16 fanout) {
    assert(fanout <= AKU_NBTREE_FANOUT);
    fanout_index_ = fanout;
    SubtreeRef* subtree = subtree_cast(block_->get_data(0));
    subtree->fanout_index = fanout;
}

LogicAddr IOVecLeaf::get_addr() const {
    return block_->get_addr();
}

LogicAddr IOVecLeaf::get_prev_addr() const {
    // Should be set correctly no metter how IOVecLeaf was created.
    return prev_;
}


aku_Status IOVecLeaf::read_all(std::vector<aku_Timestamp>* timestamps,
                               std::vector<double>* values) const
{
    int windex = writer_.get_write_index();
    IOVecBlockReader<IOVecBlock> reader(block_.get(), static_cast<u32>(sizeof(SubtreeRef)));
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

aku_Status IOVecLeaf::append(aku_Timestamp ts, double value) {
    aku_Status status = writer_.put(ts, value);
    if (status == AKU_SUCCESS) {
        SubtreeRef* subtree = subtree_cast(block_->get_data(0));
        subtree->end = ts;
        subtree->last = value;
        if (subtree->count == 0) {
            subtree->begin = ts;
            subtree->first = value;
        }
        subtree->count++;
        subtree->sum += value;
        if (subtree->max < value) {
            subtree->max = value;
            subtree->max_time = ts;
        }
        if (subtree->min > value) {
            subtree->min = value;
            subtree->min_time = ts;
        }
    }
    return status;
}

std::tuple<aku_Status, LogicAddr> IOVecLeaf::commit(std::shared_ptr<BlockStore> bstore) {
    assert(nelements() != 0);
    u16 size = static_cast<u16>(writer_.commit()) - sizeof(SubtreeRef);
    assert(size);
    SubtreeRef* subtree = subtree_cast(block_->get_data(0));
    subtree->payload_size = size;
    if (prev_ != EMPTY_ADDR && fanout_index_ > 0) {
        subtree->addr = prev_;
    } else {
        // addr = EMPTY indicates that there is
        // no link to previous node.
        subtree->addr  = EMPTY_ADDR;
        // Invariant: fanout index should be 0 in this case.
    }
    subtree->version = AKUMULI_VERSION;
    subtree->level = 0;
    subtree->type  = NBTreeBlockType::LEAF;
    subtree->fanout_index = fanout_index_;
    // Compute checksum
    subtree->checksum = bstore->checksum(*block_, sizeof(SubtreeRef), size);
    return bstore->append_block(block_);
}


std::unique_ptr<RealValuedOperator> IOVecLeaf::range(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<RealValuedOperator> it;
    it.reset(new NBTreeLeafIterator(begin, end, *this));
    return it;
}

std::unique_ptr<RealValuedOperator> IOVecLeaf::filter(aku_Timestamp begin,
                                                      aku_Timestamp end,
                                                      const ValueFilter& filter) const
{
    std::unique_ptr<RealValuedOperator> it;
    it.reset(new NBTreeLeafFilter(begin, end, filter, *this));
    return it;
}

std::unique_ptr<AggregateOperator> IOVecLeaf::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<AggregateOperator> it;
    it.reset(new NBTreeLeafAggregator(begin, end, *this));
    return it;
}

std::unique_ptr<AggregateOperator> IOVecLeaf::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    AKU_UNUSED(hint);
    auto agg = INIT_AGGRES;
    const SubtreeRef* subtree = subtree_cast(block_->get_cdata(0));
    agg.copy_from(*subtree);
    std::unique_ptr<AggregateOperator> result;
    AggregateOperator::Direction dir = begin < end ? AggregateOperator::Direction::FORWARD : AggregateOperator::Direction::BACKWARD;
    result.reset(new ValueAggregator(subtree->end, agg, dir));
    return result;
}

std::unique_ptr<AggregateOperator> IOVecLeaf::group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const {
    std::unique_ptr<AggregateOperator> it;
    it.reset(new NBTreeLeafGroupAggregator(begin, end, step, *this));
    return it;
}

std::tuple<aku_Status, LogicAddr> IOVecLeaf::split_into(std::shared_ptr<BlockStore> bstore,
                                                         aku_Timestamp pivot,
                                                         bool preserve_backrefs,
                                                         u16 *fanout_index,
                                                         NBTreeSuperblock* top_level)
{
    /* When the method is called from NBTreeSuperblock::split method, the
     * top_level node will be provided. Otherwise it will be null.
     */
    aku_Status status;
    std::vector<double> xss;
    std::vector<aku_Timestamp> tss;
    status = read_all(&tss, &xss);
    if (status != AKU_SUCCESS || tss.size() == 0) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    // Make new superblock with two leafs
    // Left hand side leaf node
    u32 ixbase = 0;
    IOVecLeaf lhs(get_id(), preserve_backrefs ? prev_ : EMPTY_ADDR, *fanout_index);
    for (u32 i = 0; i < tss.size(); i++) {
        if (tss[i] < pivot) {
            status = lhs.append(tss[i], xss[i]);
            if (status != AKU_SUCCESS) {
                return std::make_tuple(status, EMPTY_ADDR);
            }
        } else {
            ixbase = i;
            break;
        }
    }
    SubtreeRef lhs_ref;
    if (ixbase == 0) {
        // Special case, the lhs node is empty
        lhs_ref.addr = EMPTY_ADDR;
    } else {
        LogicAddr lhs_addr;
        std::tie(status, lhs_addr) = lhs.commit(bstore);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        status = init_subtree_from_leaf(lhs, lhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        lhs_ref.addr = lhs_addr;
        (*fanout_index)++;
    }
    // Right hand side leaf node, it can't be empty in any case
    // because the leaf node is not empty.
    auto prev = lhs_ref.addr == EMPTY_ADDR ? prev_ : lhs_ref.addr;
    IOVecLeaf rhs(get_id(), prev, *fanout_index);
    for (u32 i = ixbase; i < tss.size(); i++) {
        status = rhs.append(tss[i], xss[i]);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    SubtreeRef rhs_ref;
    if (ixbase == tss.size()) {
        // Special case, rhs is empty
        rhs_ref.addr = EMPTY_ADDR;
    } else {
        LogicAddr rhs_addr;
        std::tie(status, rhs_addr) = rhs.commit(bstore);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        status = init_subtree_from_leaf(rhs, rhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        rhs_ref.addr = rhs_addr;
        (*fanout_index)++;
    }
    // Superblock
    if (lhs_ref.addr != EMPTY_ADDR) {
        status = top_level->append(lhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    if (rhs_ref.addr != EMPTY_ADDR) {
        status = top_level->append(rhs_ref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
    }
    return std::make_tuple(AKU_SUCCESS, EMPTY_ADDR);
}

std::tuple<aku_Status, LogicAddr> IOVecLeaf::split(std::shared_ptr<BlockStore> bstore,
                                                    aku_Timestamp pivot,
                                                    bool preserve_backrefs)
{
    // New superblock
    NBTreeSuperblock sblock(get_id(), preserve_backrefs ? get_prev_addr() : EMPTY_ADDR, get_fanout(), 0);
    aku_Status status;
    LogicAddr  addr;
    u16 fanout = 0;
    std::tie(status, addr) = split_into(bstore, pivot, false, &fanout, &sblock);
    if (status != AKU_SUCCESS || sblock.nelements() == 0) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    std::tie(status, addr) = sblock.commit(bstore);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    return std::make_tuple(AKU_SUCCESS, addr);
}


// //////////////////////// //
//     NBTreeSuperblock     //
// //////////////////////// //

NBTreeSuperblock::NBTreeSuperblock(aku_ParamId id, LogicAddr prev, u16 fanout, u16 lvl)
    : block_(std::make_shared<Block>())
    , id_(id)
    , write_pos_(0)
    , fanout_index_(fanout)
    , level_(lvl)
    , prev_(prev)
    , immutable_(false)
{
    SubtreeRef* pref = subtree_cast(block_->get_data());
    pref->type = NBTreeBlockType::INNER;
    assert(prev_ != 0);
}

NBTreeSuperblock::NBTreeSuperblock(std::shared_ptr<Block> block)
    : block_(block)
    , immutable_(true)
{
    // Use zero-copy here.
    SubtreeRef const* ref = subtree_cast(block->get_cdata());
    assert(ref->type == NBTreeBlockType::INNER);
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    write_pos_ = ref->payload_size;
    level_ = ref->level;
    assert(prev_ != 0);
}

NBTreeSuperblock::NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore)
    : NBTreeSuperblock(read_block_from_bstore(bstore, addr))
{
}

NBTreeSuperblock::NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore, bool remove_last)
    : block_(std::make_shared<Block>())
    , immutable_(false)
{
    std::shared_ptr<Block> block = read_block_from_bstore(bstore, addr);
    SubtreeRef const* ref = subtree_cast(block->get_cdata());
    assert(ref->type == NBTreeBlockType::INNER);
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    level_ = ref->level;
    write_pos_ = ref->payload_size;
    if (remove_last && write_pos_ != 0) {
        write_pos_--;
    }
    assert(prev_ != 0);
    // We can't use zero-copy here because `block` belongs to other node.
    memcpy(block_->get_data(), block->get_cdata(), AKU_BLOCK_SIZE);
}

SubtreeRef const* NBTreeSuperblock::get_sblockmeta() const {
    SubtreeRef const* pref = subtree_cast(block_->get_cdata());
    return pref;
}

size_t NBTreeSuperblock::nelements() const {
    return write_pos_;
}

u16 NBTreeSuperblock::get_level() const {
    return level_;
}

u16 NBTreeSuperblock::get_fanout() const {
    return fanout_index_;
}

aku_ParamId NBTreeSuperblock::get_id() const {
    return id_;
}

LogicAddr NBTreeSuperblock::get_prev_addr() const {
    return prev_;
}

void NBTreeSuperblock::set_prev_addr(LogicAddr addr) {
    assert(addr != 0);
    prev_ = addr;
    subtree_cast(block_->get_data())->addr = addr;
}

void NBTreeSuperblock::set_node_fanout(u16 newfanout) {
    assert(newfanout <= AKU_NBTREE_FANOUT);
    fanout_index_ = newfanout;
    subtree_cast(block_->get_data())->fanout_index = newfanout;
}

LogicAddr NBTreeSuperblock::get_addr() const {
    return block_->get_addr();
}

aku_Status NBTreeSuperblock::append(const SubtreeRef &p) {
    if (is_full()) {
        return AKU_EOVERFLOW;
    }
    if (immutable_) {
        return AKU_EBAD_DATA;
    }
    // Write data into buffer
    SubtreeRef* pref = subtree_cast(block_->get_data());
    auto it = pref + 1 + write_pos_;
    *it = p;
    if (write_pos_ == 0) {
        pref->begin = p.begin;
    }
    pref->end = p.end;
    write_pos_++;
    return AKU_SUCCESS;
}

std::tuple<aku_Status, LogicAddr> NBTreeSuperblock::commit(std::shared_ptr<BlockStore> bstore) {
    assert(nelements() != 0);
    if (immutable_) {
        return std::make_tuple(AKU_EBAD_DATA, EMPTY_ADDR);
    }
    SubtreeRef* backref = subtree_cast(block_->get_data());
    auto status = init_subtree_from_subtree(*this, *backref);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    backref->addr = prev_;
    backref->payload_size = static_cast<u16>(write_pos_);
    assert(backref->payload_size + sizeof(SubtreeRef) < AKU_BLOCK_SIZE);
    backref->fanout_index = fanout_index_;
    backref->id = id_;
    backref->level = level_;
    backref->type  = NBTreeBlockType::INNER;
    backref->version = AKUMULI_VERSION;
    // add checksum
    backref->checksum = bstore->checksum(block_->get_cdata() + sizeof(SubtreeRef), backref->payload_size);
    return bstore->append_block(block_);
}

bool NBTreeSuperblock::is_full() const {
    return write_pos_ >= AKU_NBTREE_FANOUT;
}

aku_Status NBTreeSuperblock::read_all(std::vector<SubtreeRef>* refs) const {
    SubtreeRef const* ref = subtree_cast(block_->get_cdata());
    for(u32 ix = 0u; ix < write_pos_; ix++) {
        auto p = ref + 1 + ix;
        refs->push_back(*p);
    }
    return AKU_SUCCESS;
}

bool NBTreeSuperblock::top(SubtreeRef* outref) const {
    if (write_pos_ == 0) {
        return false;
    }
    SubtreeRef const* ref = subtree_cast(block_->get_cdata());
    auto p = ref + write_pos_;
    *outref = *p;
    return true;
}

bool NBTreeSuperblock::top(LogicAddr* outaddr) const {
    SubtreeRef child;
    if (top(&child)) {
        *outaddr = child.addr;
        return true;
    }
    return false;
}

std::tuple<aku_Timestamp, aku_Timestamp> NBTreeSuperblock::get_timestamps() const {
    SubtreeRef const* pref = subtree_cast(block_->get_cdata());
    return std::tie(pref->begin, pref->end);
}

std::unique_ptr<RealValuedOperator> NBTreeSuperblock::search(aku_Timestamp begin,
                                                         aku_Timestamp end,
                                                         std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<RealValuedOperator> result;
    result.reset(new NBTreeSBlockIterator(bstore, *this, begin, end));
    return result;
}

std::unique_ptr<RealValuedOperator> NBTreeSuperblock::filter(aku_Timestamp begin,
                                                             aku_Timestamp end,
                                                             const ValueFilter& filter,
                                                             std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<RealValuedOperator> result;
    result.reset(new NBTreeSBlockFilter(bstore, *this, begin, end, filter));
    return result;
}

std::unique_ptr<AggregateOperator> NBTreeSuperblock::aggregate(aku_Timestamp begin,
                                                            aku_Timestamp end,
                                                            std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeSBlockAggregator(bstore, *this, begin, end));
    return result;
}

std::unique_ptr<AggregateOperator> NBTreeSuperblock::candlesticks(aku_Timestamp begin, aku_Timestamp end,
                                                                 std::shared_ptr<BlockStore> bstore,
                                                                 NBTreeCandlestickHint hint) const
{
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeSBlockCandlesticsIter(bstore, *this, begin, end, hint));
    return result;
}

std::unique_ptr<AggregateOperator> NBTreeSuperblock::group_aggregate(aku_Timestamp begin,
                                                                    aku_Timestamp end,
                                                                    u64 step,
                                                                    std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeSBlockGroupAggregator(bstore, *this, begin, end, step));
    return result;
}

std::tuple<aku_Status, LogicAddr> NBTreeSuperblock::split_into(std::shared_ptr<BlockStore> bstore,
                                                                          aku_Timestamp pivot,
                                                                          bool preserve_horizontal_links,
                                                                          NBTreeSuperblock* root)
{
    // for each node in BFS order:
    //      if pivot is inside the node:
    //          node.split() <-- recursive call
    //      else if top_level_node and node is on the right from pivot:
    //          node.clone().fix_horizontal_link()
    std::vector<SubtreeRef> refs;
    aku_Status status = read_all(&refs);
    if (status != AKU_SUCCESS || refs.empty()) {
        return std::make_tuple(status, EMPTY_ADDR);
    }
    for (u32 i = 0; i < refs.size(); i++) {
        if (refs[i].begin <= pivot && pivot <= refs[i].end) {
            // Do split the node
            LogicAddr new_ith_child_addr = EMPTY_ADDR;
            u16 current_fanout = 0;
            // Clone current node
            for (u32 j = 0; j < i; j++) {
                root->append(refs[j]);
                current_fanout++;
            }
            std::shared_ptr<Block> block;
            std::tie(status, block) = read_and_check(bstore, refs[i].addr);
            if (status != AKU_SUCCESS) {
                return std::make_tuple(status, EMPTY_ADDR);
            }
            auto refsi = subtree_cast(block->get_cdata());
            assert(refsi->count == refs[i].count);
            assert(refsi->type  == refs[i].type);
            assert(refsi->begin == refs[i].begin);
            AKU_UNUSED(refsi);
            if (refs[i].type == NBTreeBlockType::INNER) {
                NBTreeSuperblock sblock(block);
                LogicAddr ignored;
                std::tie(status, new_ith_child_addr, ignored) = sblock.split(bstore, pivot, false);
                if (status != AKU_SUCCESS) {
                    return std::make_tuple(status, EMPTY_ADDR);
                }
            } else {
                NBTreeLeaf oldleaf(block);
                if ((refs.size() - AKU_NBTREE_FANOUT) > 1) {
                    // Split in-place
                    std::tie(status, new_ith_child_addr) = oldleaf.split_into(bstore, pivot, preserve_horizontal_links, &current_fanout, root);
                    if (status != AKU_SUCCESS) {
                        return std::make_tuple(status, EMPTY_ADDR);
                    }
                } else {
                    // Create new level in the tree
                    std::tie(status, new_ith_child_addr) = oldleaf.split(bstore, pivot, preserve_horizontal_links);
                    if (status != AKU_SUCCESS) {
                        return std::make_tuple(status, EMPTY_ADDR);
                    }
                }
            }
            if (new_ith_child_addr != EMPTY_ADDR) {
                SubtreeRef newref;
                std::shared_ptr<Block> block = read_block_from_bstore(bstore, new_ith_child_addr);
                NBTreeSuperblock child(block);
                status = init_subtree_from_subtree(child, newref);
                if (status != AKU_SUCCESS) {
                    return std::make_tuple(status, EMPTY_ADDR);
                }
                newref.addr = new_ith_child_addr;
                root->append(newref);
                current_fanout++;
            }
            LogicAddr last_child_addr;
            if (!root->top(&last_child_addr)) {
                AKU_PANIC("Attempt to split an empty node");
            }
            if (preserve_horizontal_links) {
                // Fix backrefs on the right from the pivot
                // Move from left to right and clone the blocks fixing
                // the back references.
                for (u32 j = i+1; j < refs.size(); j++) {
                    if (refs[j].type == NBTreeBlockType::INNER) {
                        NBTreeSuperblock cloned_child(refs[j].addr, bstore, false);
                        cloned_child.set_prev_addr(last_child_addr);
                        cloned_child.set_node_fanout(current_fanout);
                        current_fanout++;
                        std::tie(status, last_child_addr) = cloned_child.commit(bstore);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                        SubtreeRef backref;
                        status = init_subtree_from_subtree(cloned_child, backref);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                        backref.addr = last_child_addr;
                        status = root->append(backref);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                    } else {
                        std::shared_ptr<Block> child_block;
                        std::tie(status, child_block) = read_and_check(bstore, refs[j].addr);
                        NBTreeLeaf cloned_child(child_block, NBTreeLeaf::CloneTag());
                        cloned_child.set_prev_addr(last_child_addr);
                        cloned_child.set_node_fanout(current_fanout);
                        current_fanout++;
                        std::tie(status, last_child_addr) = cloned_child.commit(bstore);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                        SubtreeRef backref;
                        status = init_subtree_from_leaf(cloned_child, backref);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                        backref.addr = last_child_addr;
                        status = root->append(backref);
                        if (status != AKU_SUCCESS) {
                            return std::make_tuple(status, EMPTY_ADDR);
                        }
                    }
                }
            } else {
                for (u32 j = i+1; j < refs.size(); j++) {
                    root->append(refs[j]);
                }
            }
            return std::tie(status, last_child_addr);
        }
    }
    // The pivot point is not found
    return std::make_tuple(AKU_ENOT_FOUND, EMPTY_ADDR);
}

std::tuple<aku_Status, LogicAddr, LogicAddr> NBTreeSuperblock::split(std::shared_ptr<BlockStore> bstore,
                                                                     aku_Timestamp pivot,
                                                                     bool preserve_horizontal_links)
{
    aku_Status status;
    LogicAddr last_child;
    NBTreeSuperblock new_sblock(id_, prev_, get_fanout(), level_);
    std::tie(status, last_child) = split_into(bstore, pivot, preserve_horizontal_links, &new_sblock);
    if (status != AKU_SUCCESS || new_sblock.nelements() == 0) {
        return std::make_tuple(status, EMPTY_ADDR, EMPTY_ADDR);
    }
    LogicAddr newaddr = EMPTY_ADDR;
    std::tie(status, newaddr) = new_sblock.commit(bstore);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, EMPTY_ADDR, EMPTY_ADDR);
    }
    return std::tie(status, newaddr, last_child);
}
// //////////////////////// //
//        NBTreeExtent      //
// //////////////////////// //


//! Represents extent made of one memory resident leaf node
struct NBTreeLeafExtent : NBTreeExtent {
    std::shared_ptr<BlockStore> bstore_;
    std::weak_ptr<NBTreeExtentsList> roots_;
    aku_ParamId id_;
    LogicAddr last_;
    std::shared_ptr<IOVecLeaf> leaf_;
    u16 fanout_index_;
    // padding
    u16 pad0_;
    u32 pad1_;

    NBTreeLeafExtent(std::shared_ptr<BlockStore> bstore,
                     std::shared_ptr<NBTreeExtentsList> roots,
                     aku_ParamId id,
                     LogicAddr last)
        : bstore_(bstore)
        , roots_(roots)
        , id_(id)
        , last_(last)
        , fanout_index_(0)
        , pad0_{}
        , pad1_{}
    {
        if (last_ != EMPTY_ADDR) {
            // Load previous node and calculate fanout.
            aku_Status status;
            std::shared_ptr<Block> block;
            std::tie(status, block) = read_and_check(bstore_, last_);
            if (status == AKU_EUNAVAILABLE) {
                // Can't read previous node (retention)
                fanout_index_ = 0;
                last_ = EMPTY_ADDR;
            } else if (status != AKU_SUCCESS) {
                Logger::msg(AKU_LOG_ERROR, "Can't read block @" + std::to_string(last_) + ", error: " + StatusUtil::str(status));
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            } else {
                auto psubtree = subtree_cast(block->get_cdata());
                fanout_index_ = psubtree->fanout_index + 1;
                if (fanout_index_ == AKU_NBTREE_FANOUT) {
                    fanout_index_ = 0;
                    last_ = EMPTY_ADDR;
                }
            }
        }
        reset_leaf();
    }

    size_t bytes_used() const {
        return leaf_->bytes_used();
    }

    virtual ExtentStatus status() const override {
        // Leaf extent can be new and empty or new and filled with data
        if (leaf_->nelements() == 0) {
            return ExtentStatus::NEW;
        }
        return ExtentStatus::OK;
    }

    virtual aku_Status update_prev_addr(LogicAddr addr) override {
        if (leaf_->get_addr() == EMPTY_ADDR) {
            leaf_->set_prev_addr(addr);
            return AKU_SUCCESS;
        }
        // This can happen due to concurrent access
        return AKU_EACCESS;
    }

    virtual aku_Status update_fanout_index(u16 fanout_index) override {
        if (leaf_->get_addr() == EMPTY_ADDR) {
            leaf_->set_node_fanout(fanout_index);
            fanout_index_ = fanout_index;
            return AKU_SUCCESS;
        }
        // This can happen due to concurrent access
        return AKU_EACCESS;
    }

    aku_Status get_prev_subtreeref(SubtreeRef &payload) {
        aku_Status status = AKU_SUCCESS;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, last_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        NBTreeLeaf leaf(block);
        status = init_subtree_from_leaf(leaf, payload);
        payload.addr = last_;
        return status;
    }

    u16 get_current_fanout_index() const {
        return leaf_->get_fanout();
    }

    void reset_leaf() {
        //leaf_.reset(new NBTreeLeaf(id_, last_, fanout_index_));
        leaf_.reset(new IOVecLeaf(id_, last_, fanout_index_));
    }

    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value) override;
    virtual std::tuple<bool, LogicAddr> append(const SubtreeRef &pl) override;
    virtual std::tuple<bool, LogicAddr> commit(bool final) override;
    virtual std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end) const override;
    virtual std::unique_ptr<RealValuedOperator> filter(aku_Timestamp begin,
                                                       aku_Timestamp end,
                                                       const ValueFilter& filter) const override;
    virtual std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin, aku_Timestamp end) const override;
    virtual std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const override;
    virtual std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const override;
    virtual bool is_dirty() const override;
    virtual void debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat, u32 mask) const override;
    virtual std::tuple<bool, LogicAddr> split(aku_Timestamp pivot) override;
};


static void dump_subtree_ref(std::ostream& stream,
                             SubtreeRef const* ref,
                             LogicAddr prev_addr,
                             int base_indent,
                             LogicAddr self_addr,
                             std::function<std::string(aku_Timestamp)> tsformat,
                             u32 mask=0xFFFFFFFF)
{
    auto tag = [base_indent](const char* tag_name) {
        std::stringstream str;
        for (int i = 0; i < base_indent; i++) {
            str << '\t';
        }
        str << '<' << tag_name << '>';
        return str.str();
    };
    auto afmt = [](LogicAddr addr) {
        if (addr == EMPTY_ADDR) {
            return std::string();
        }
        return std::to_string(addr);
    };
    if (mask&1) {
        if (ref->type == NBTreeBlockType::LEAF) {
            stream << tag("type")     << "Leaf"                       << "</type>\n";
        } else {
            stream << tag("type")     << "Superblock"                 << "</type>\n";
        }
    }
    if (mask & 2) {
        stream << tag("addr")         << afmt(self_addr)              << "</addr>\n";
    }
    if (mask & 4) {
        stream << tag("id")           << ref->id                      << "</id>\n";
    }
    if (mask & 8) {
        stream << tag("prev_addr")    << afmt(prev_addr)              << "</prev_addr>\n";
    }
    if (mask & 16) {
        stream << tag("begin")        << tsformat(ref->begin)         << "</begin>\n";
    }
    if (mask & 32) {
        stream << tag("end")          << tsformat(ref->end)           << "</end>\n";
    }
    if (mask & 64) {
        stream << tag("count")        << ref->count                   << "</count>\n";
    }
    if (mask & 128) {
        stream << tag("min")          << ref->min                     << "</min>\n";
    }
    if (mask & 0x100) {
        stream << tag("min_time")     << tsformat(ref->min_time)      << "</min_time>\n";
    }
    if (mask & 0x200) {
        stream << tag("max")          << ref->max                     << "</max>\n";
    }
    if (mask & 0x400) {
        stream << tag("max_time")     << tsformat(ref->max_time)      << "</max_time>\n";
    }
    if (mask & 0x800) {
        stream << tag("sum")          << ref->sum                     << "</sum>\n";
    }
    if (mask & 0x1000) {
        stream << tag("first")        << ref->first                   << "</first>\n";
    }
    if (mask & 0x2000) {
        stream << tag("last")         << ref->last                    << "</last>\n";
    }
    if (mask & 0x4000) {
        stream << tag("version")      << ref->version                 << "</version>\n";
    }
    if (mask & 0x8000) {
        stream << tag("level")        << ref->level                   << "</level>\n";
    }
    if (mask & 0x10000) {
        stream << tag("type")         << ref->type                    << "</level>\n";
    }
    if (mask & 0x20000) {
        stream << tag("payload_size") << ref->payload_size            << "</payload_size>\n";
    }
    if (mask & 0x40000) {
        stream << tag("fanout_index") << ref->fanout_index            << "</fanout_index>\n";
    }
    if (mask & 0x80000) {
        stream << tag("checksum")     << ref->checksum                << "</checksum>\n";
    }
}


void NBTreeLeafExtent::debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat, u32 mask) const {
    SubtreeRef const* ref = leaf_->get_leafmeta();
    stream << std::string(static_cast<size_t>(base_indent), '\t') <<  "<node>\n";
    dump_subtree_ref(stream, ref, leaf_->get_prev_addr(), base_indent + 1, leaf_->get_addr(), tsformat, mask);
    stream << std::string(static_cast<size_t>(base_indent), '\t') << "</node>\n";
}

std::tuple<bool, LogicAddr> NBTreeLeafExtent::append(SubtreeRef const&) {
    Logger::msg(AKU_LOG_ERROR, "Attempt to insert ref into a leaf node, id=" + std::to_string(id_)
                + ", fanout=" + std::to_string(fanout_index_) + ", last=" + std::to_string(last_));
    AKU_PANIC("Can't append subtree to leaf node");
}

std::tuple<bool, LogicAddr> NBTreeLeafExtent::append(aku_Timestamp ts, double value) {
    // Invariant: leaf_ should be initialized, if leaf_ is full
    // and pushed to block-store, reset_leaf should be called
    aku_Status status = leaf_->append(ts, value);
    if (status == AKU_EOVERFLOW) {
        LogicAddr addr;
        bool parent_saved;
        // Commit full node
        std::tie(parent_saved, addr) = commit(false);
        // Stack overflow here means that there is a logic error in
        // the program that results in NBTreeLeaf::append always
        // returning AKU_EOVERFLOW.
        append(ts, value);
        return std::make_tuple(parent_saved, addr);
    }
    return std::make_tuple(false, EMPTY_ADDR);
}

//! Forcibly commit changes, even if current page is not full
std::tuple<bool, LogicAddr> NBTreeLeafExtent::commit(bool final) {
    // Invariant: after call to this method data from `leaf_` should
    // endup in block store, upper level root node should be updated
    // and `leaf_` variable should be reset.
    // Otherwise: panic should be triggered.

    LogicAddr addr;
    aku_Status status;
    std::tie(status, addr) = leaf_->commit(bstore_);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't write leaf-node to block-store, " + StatusUtil::str(status)
                    + ", id=" + std::to_string(id_) + ", fanout=" + std::to_string(fanout_index_)
                    + ", last=" + std::to_string(last_));
        AKU_PANIC("Can't write leaf-node to block-store, " + StatusUtil::str(status));
    }
    // Gather stats and send them to upper-level node
    SubtreeRef payload = INIT_SUBTREE_REF;
    status = init_subtree_from_leaf(*leaf_, payload);
    if (status != AKU_SUCCESS) {
        // This shouldn't happen because leaf node can't be
        // empty just after overflow.
        Logger::msg(AKU_LOG_ERROR, "Can summarize leaf-node - " + StatusUtil::str(status)
                    + ", id=" + std::to_string(id_) + ", fanout=" + std::to_string(fanout_index_)
                    + ", last=" + std::to_string(last_) + ", payload=" + to_string(payload));
        AKU_PANIC("Can summarize leaf-node - " + StatusUtil::str(status));
    }
    payload.addr = addr;
    bool parent_saved = false;
    auto roots_collection = roots_.lock();
    size_t next_level = payload.level + 1;
    if (roots_collection) {
        if (!final || roots_collection->_get_roots().size() > next_level) {
            parent_saved = roots_collection->append(payload);
        }
    } else {
        // Invariant broken.
        // Roots collection was destroyed before write process
        // stops.
        Logger::msg(AKU_LOG_ERROR, "Roots collection destroyed"
                    ", id=" + std::to_string(id_) + ", fanout=" + std::to_string(fanout_index_)
                    + ", last=" + std::to_string(last_) + ", payload=" + to_string(payload));
        AKU_PANIC("Roots collection destroyed");
    }
    fanout_index_++;
    last_ = addr;
    if (fanout_index_ == AKU_NBTREE_FANOUT) {
        fanout_index_ = 0;
        last_ = EMPTY_ADDR;
    }
    reset_leaf();
    // NOTE: we should reset current extent's rescue point because parent node was saved and
    // already has a link to current extent (e.g. leaf node was saved and new leaf
    // address was added to level 1 node, level 1 node becomes full and was written to disk).
    // If we won't do this - we will read the same information twice during crash recovery process.
    return std::make_tuple(parent_saved, addr);
}

std::unique_ptr<RealValuedOperator> NBTreeLeafExtent::search(aku_Timestamp begin, aku_Timestamp end) const {
    return leaf_->range(begin, end);
}

std::unique_ptr<RealValuedOperator> NBTreeLeafExtent::filter(aku_Timestamp begin,
                                                             aku_Timestamp end,
                                                             const ValueFilter& filter) const
{
    return leaf_->filter(begin, end, filter);
}

std::unique_ptr<AggregateOperator> NBTreeLeafExtent::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    return leaf_->aggregate(begin, end);
}

std::unique_ptr<AggregateOperator> NBTreeLeafExtent::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    return leaf_->candlesticks(begin, end, hint);
}

std::unique_ptr<AggregateOperator> NBTreeLeafExtent::group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const {
    return leaf_->group_aggregate(begin, end, step);
}

bool NBTreeLeafExtent::is_dirty() const {
    if (leaf_) {
        return leaf_->nelements() != 0;
    }
    return false;
}

std::tuple<bool, LogicAddr> NBTreeLeafExtent::split(aku_Timestamp pivot) {
    aku_Status status;
    LogicAddr addr;
    std::tie(status, addr) = leaf_->split(bstore_, pivot, true);
    if (status != AKU_SUCCESS || addr == EMPTY_ADDR) {
        return std::make_tuple(false, EMPTY_ADDR);
    }
    auto block = read_block_from_bstore(bstore_, addr);
    NBTreeSuperblock sblock(block);
    // Gather stats and send them to upper-level node
    SubtreeRef payload = INIT_SUBTREE_REF;
    status = init_subtree_from_subtree(sblock, payload);
    if (status != AKU_SUCCESS) {
        // This shouldn't happen because sblock can't be empty, it contains
        // two or one child element.
        Logger::msg(AKU_LOG_ERROR, "Can summarize leaf-node - " + StatusUtil::str(status)
                    + ", id=" + std::to_string(id_) + ", fanout=" + std::to_string(fanout_index_)
                    + ", last=" + std::to_string(last_));
        AKU_PANIC("Can summarize leaf-node - " + StatusUtil::str(status));
    }
    payload.addr = addr;
    bool parent_saved = false;
    auto roots_collection = roots_.lock();
    if (roots_collection) {
        parent_saved = roots_collection->append(payload);
    } else {
        // Invariant broken.
        // Roots collection was destroyed before write process
        // stops.
        Logger::msg(AKU_LOG_ERROR, "Roots collection destroyed"
                    ", id=" + std::to_string(id_) + ", fanout=" + std::to_string(fanout_index_)
                    + ", last=" + std::to_string(last_));
        AKU_PANIC("Roots collection destroyed");
    }
    fanout_index_++;
    last_ = addr;
    if (fanout_index_ == AKU_NBTREE_FANOUT) {
        fanout_index_ = 0;
        last_ = EMPTY_ADDR;
    }
    reset_leaf();
    return std::make_tuple(parent_saved, addr);
}

// ////////////////////// //
//   NBTreeSBlockExtent   //
// ////////////////////// //

struct NBTreeSBlockExtent : NBTreeExtent {
    std::shared_ptr<BlockStore> bstore_;
    std::weak_ptr<NBTreeExtentsList> roots_;
    std::unique_ptr<NBTreeSuperblock> curr_;
    aku_ParamId id_;
    LogicAddr last_;
    u16 fanout_index_;
    u16 level_;
    // padding
    u32 killed_;

    NBTreeSBlockExtent(std::shared_ptr<BlockStore> bstore,
                       std::shared_ptr<NBTreeExtentsList> roots,
                       aku_ParamId id,
                       LogicAddr addr,
                       u16 level)
        : bstore_(bstore)
        , roots_(roots)
        , id_(id)
        , last_(EMPTY_ADDR)
        , fanout_index_(0)
        , level_(level)
        , killed_(0)
    {
        if (addr != EMPTY_ADDR) {
            // `addr` is not empty. Node should be restored from
            // block-store.
            aku_Status status;
            std::shared_ptr<Block> block;
            std::tie(status, block) = read_and_check(bstore_, addr);
            if (status  == AKU_EUNAVAILABLE) {
                addr = EMPTY_ADDR;
                killed_ = 1;
            } else if (status != AKU_SUCCESS) {
                Logger::msg(AKU_LOG_ERROR, "Can't read @" + std::to_string(addr) + ", error: " + StatusUtil::str(status));
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            } else {
                auto psubtree = subtree_cast(block->get_cdata());
                fanout_index_ = psubtree->fanout_index + 1;
                if (fanout_index_ == AKU_NBTREE_FANOUT) {
                    fanout_index_ = 0;
                    last_ = EMPTY_ADDR;
                }
                last_ = psubtree->addr;
            }
        }
        if (addr != EMPTY_ADDR) {
            // CoW constructor should be used here.
            curr_.reset(new NBTreeSuperblock(addr, bstore_, false));
        } else {
            // `addr` is not set. Node should be created from scratch.
            curr_.reset(new NBTreeSuperblock(id, EMPTY_ADDR, 0, level));
        }
    }

    ExtentStatus status() const override {
        if (killed_) {
            return ExtentStatus::KILLED_BY_RETENTION;
        } else if (curr_->nelements() == 0) {
            // Node is new
            return ExtentStatus::NEW;
        }
        // Node is filled with data or created using CoW constructor
        return ExtentStatus::OK;
    }

    virtual aku_Status update_prev_addr(LogicAddr addr) override {
        if (curr_->get_addr() == EMPTY_ADDR) {
            curr_->set_prev_addr(addr);
            return AKU_SUCCESS;
        }
        return AKU_EACCESS;
    }

    virtual aku_Status update_fanout_index(u16 fanout_index) override {
        if (curr_->get_addr() == EMPTY_ADDR) {
            curr_->set_node_fanout(fanout_index);
            fanout_index_ = fanout_index;
            return AKU_SUCCESS;
        }
        return AKU_EACCESS;
    }

    void reset_subtree() {
        curr_.reset(new NBTreeSuperblock(id_, last_, fanout_index_, level_));
    }

    u16 get_fanout_index() const {
        return fanout_index_;
    }

    u16 get_level() const {
        return level_;
    }

    LogicAddr get_prev_addr() const {
        return curr_->get_prev_addr();
    }

    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value) override;
    virtual std::tuple<bool, LogicAddr> append(const SubtreeRef &pl) override;
    virtual std::tuple<bool, LogicAddr> commit(bool final) override;
    virtual std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end) const override;
    virtual std::unique_ptr<RealValuedOperator> filter(aku_Timestamp begin,
                                                       aku_Timestamp end,
                                                       const ValueFilter& filter) const override;
    virtual std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin, aku_Timestamp end) const override;
    virtual std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const override;
    virtual std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const override;
    virtual bool is_dirty() const override;
    virtual void debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat, u32 mask) const override;
    virtual std::tuple<bool, LogicAddr> split(aku_Timestamp pivot) override;
};

void NBTreeSBlockExtent::debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat, u32 mask) const {
    SubtreeRef const* ref = curr_->get_sblockmeta();
    stream << std::string(static_cast<size_t>(base_indent), '\t') <<  "<node>\n";
    dump_subtree_ref(stream, ref, curr_->get_prev_addr(), base_indent + 1, curr_->get_addr(), tsformat, mask);

    std::vector<SubtreeRef> refs;
    aku_Status status = curr_->read_all(&refs);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, std::string("Can't read data ") + StatusUtil::str(status));
        return;
    }

    if (refs.empty()) {
        stream << std::string(static_cast<size_t>(base_indent), '\t') <<  "</node>\n";
        return;
    }

    // Traversal control
    enum class Action {
        DUMP_NODE,
        OPEN_NODE,
        CLOSE_NODE,
        OPEN_CHILDREN,
        CLOSE_CHILDREN,
    };

    typedef std::tuple<LogicAddr, Action, int> StackItem;

    std::stack<StackItem> stack;

    stack.push(std::make_tuple(0, Action::CLOSE_NODE, base_indent));
    stack.push(std::make_tuple(0, Action::CLOSE_CHILDREN, base_indent + 1));
    for (auto const& ref: refs) {
        LogicAddr addr = ref.addr;
        stack.push(std::make_tuple(0, Action::CLOSE_NODE, base_indent + 2));
        stack.push(std::make_tuple(addr, Action::DUMP_NODE, base_indent + 3));
        stack.push(std::make_tuple(0, Action::OPEN_NODE, base_indent + 2));
    }
    stack.push(std::make_tuple(0, Action::OPEN_CHILDREN, base_indent + 1));

    // Tree traversal (depth first)
    while(!stack.empty()) {
        LogicAddr addr;
        Action action;
        int indent;
        std::tie(addr, action, indent) = stack.top();
        stack.pop();

        auto tag = [indent](const char* tag_name, const char* tag_opener = "<") {
            return std::string(static_cast<size_t>(indent), '\t') + tag_opener + tag_name + ">";
        };

        switch(action) {
        case Action::DUMP_NODE: {
            aku_Status status;
            std::shared_ptr<Block> block;
            std::tie(status, block) = bstore_->read_block(addr);
            if (status != AKU_SUCCESS) {
                stream << tag("addr") << addr << "</addr>\n";
                stream << tag("fail") << StatusUtil::c_str(status) << "</fail>" << std::endl;
                continue;
            }
            auto subtreeref = reinterpret_cast<const SubtreeRef*>(block->get_cdata());
            if (subtreeref->type == NBTreeBlockType::LEAF) {
                // leaf node
                NBTreeLeaf leaf(block);
                SubtreeRef const* ref = leaf.get_leafmeta();
                dump_subtree_ref(stream, ref, leaf.get_prev_addr(), indent, leaf.get_addr(), tsformat, mask);
            } else {
                // superblock
                NBTreeSuperblock sblock(block);
                SubtreeRef const* ref = sblock.get_sblockmeta();
                dump_subtree_ref(stream, ref, sblock.get_prev_addr(), indent, sblock.get_addr(), tsformat, mask);
                std::vector<SubtreeRef> children;
                status = sblock.read_all(&children);
                if (status != AKU_SUCCESS) {
                    AKU_PANIC("Can't read superblock");
                }
                stack.push(std::make_tuple(0, Action::CLOSE_CHILDREN, indent));
                for(const SubtreeRef& sref: children) {
                    stack.push(std::make_tuple(0, Action::CLOSE_NODE, indent + 1));
                    stack.push(std::make_tuple(sref.addr, Action::DUMP_NODE, indent + 2));
                    stack.push(std::make_tuple(0, Action::OPEN_NODE, indent + 1));
                }
                stack.push(std::make_tuple(0, Action::OPEN_CHILDREN, indent));
            }
        }
        break;
        case Action::OPEN_NODE:
            stream << tag("node") << std::endl;
        break;
        case Action::CLOSE_NODE:
            stream << tag("node", "</") << std::endl;
        break;
        case Action::OPEN_CHILDREN:
            stream << tag("children") << std::endl;
        break;
        case Action::CLOSE_CHILDREN:
            stream << tag("children", "</") << std::endl;
        break;
        };
    }
}

std::tuple<bool, LogicAddr> NBTreeSBlockExtent::append(aku_Timestamp, double) {
    AKU_PANIC("Data should be added to the root 0");
}

std::tuple<bool, LogicAddr> NBTreeSBlockExtent::append(SubtreeRef const& pl) {
    auto status = curr_->append(pl);
    if (status == AKU_EOVERFLOW) {
        LogicAddr addr;
        bool parent_saved;
        std::tie(parent_saved, addr) = commit(false);
        append(pl);
        return std::make_tuple(parent_saved, addr);
    }
    return std::make_tuple(false, EMPTY_ADDR);
}

std::tuple<bool, LogicAddr> NBTreeSBlockExtent::commit(bool final) {
    // Invariant: after call to this method data from `curr_` should
    // endup in block store, upper level root node should be updated
    // and `curr_` variable should be reset.
    // Otherwise: panic should be triggered.

    LogicAddr addr;
    aku_Status status;
    std::tie(status, addr) = curr_->commit(bstore_);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can't write superblock to block-store, " + StatusUtil::str(status));
    }
    // Gather stats and send them to upper-level node
    SubtreeRef payload = INIT_SUBTREE_REF;
    status = init_subtree_from_subtree(*curr_, payload);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can summarize current node - " + StatusUtil::str(status));
    }
    payload.addr = addr;
    bool parent_saved = false;
    auto roots_collection = roots_.lock();
    size_t next_level = payload.level + 1;
    if (roots_collection) {
        if (!final || roots_collection->_get_roots().size() > next_level) {
            // We shouldn't create new root if `commit` called from `close` method.
            parent_saved = roots_collection->append(payload);
        }
    } else {
        // Invariant broken.
        // Roots collection was destroyed before write process
        // stops.
        AKU_PANIC("Roots collection destroyed");
    }
    fanout_index_++;
    last_ = addr;
    if (fanout_index_ == AKU_NBTREE_FANOUT) {
        fanout_index_ = 0;
        last_ = EMPTY_ADDR;
    }
    reset_subtree();
    // NOTE: we should reset current extent's rescue point because parent node was saved and
    // parent node already has a link to this extent.
    return std::make_tuple(parent_saved, addr);
}

std::unique_ptr<RealValuedOperator> NBTreeSBlockExtent::search(aku_Timestamp begin, aku_Timestamp end) const {
    return curr_->search(begin, end, bstore_);
}

std::unique_ptr<RealValuedOperator> NBTreeSBlockExtent::filter(aku_Timestamp begin,
                                                               aku_Timestamp end,
                                                               const ValueFilter& filter) const
{
    return curr_->filter(begin, end, filter, bstore_);
}

std::unique_ptr<AggregateOperator> NBTreeSBlockExtent::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    return curr_->aggregate(begin, end, bstore_);
}

std::unique_ptr<AggregateOperator> NBTreeSBlockExtent::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    return curr_->candlesticks(begin, end, bstore_, hint);
}

std::unique_ptr<AggregateOperator> NBTreeSBlockExtent::group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const {
    return curr_->group_aggregate(begin, end, step, bstore_);
}

bool NBTreeSBlockExtent::is_dirty() const {
    if (curr_) {
        return curr_->nelements() != 0;
    }
    return false;
}

std::tuple<bool, LogicAddr> NBTreeSBlockExtent::split(aku_Timestamp pivot) {
    const auto empty_res = std::make_tuple(false, EMPTY_ADDR);
    aku_Status status;
    std::unique_ptr<NBTreeSuperblock> clone;
    clone.reset(new NBTreeSuperblock(id_, curr_->get_prev_addr(), curr_->get_fanout(), curr_->get_level()));
    LogicAddr last_child_addr;
    std::tie(status, last_child_addr) = curr_->split_into(bstore_, pivot, true, clone.get());
    // The addr variable should be empty, because we're using the clone
    if (status != AKU_SUCCESS) {
        return empty_res;
    }
    curr_.swap(clone);
    return std::make_tuple(false, last_child_addr);
}


static void check_superblock_consistency(std::shared_ptr<BlockStore> bstore,
                                         NBTreeSuperblock const* sblock,
                                         u16 required_level,
                                         bool check_backrefs) {
    // For each child.
    std::vector<SubtreeRef> refs;
    aku_Status status = sblock->read_all(&refs);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("NBTreeSuperblock.read_all failed, exit code: " + StatusUtil::str(status));
    }
    std::vector<LogicAddr> nodes2follow;
    // Check nodes.
    size_t nelements = sblock->nelements();
    int nerrors = 0;
    for (size_t i = 0; i < nelements; i++) {
        if (check_backrefs) {
            // require refs[i].fanout_index == i.
            auto fanout = refs[i].fanout_index;
            if (fanout != i) {
                std::string error_message = "Faulty superblock found, expected fanout_index = "
                                          + std::to_string(i) + " actual = "
                                          + std::to_string(fanout);
                Logger::msg(AKU_LOG_ERROR, error_message);
                nerrors++;
            }
            if (refs[i].level != required_level) {
                std::string error_message = "Faulty superblock found, expected level = "
                                          + std::to_string(required_level) + " actual level = "
                                          + std::to_string(refs[i].level);
                Logger::msg(AKU_LOG_ERROR, error_message);
                nerrors++;
            }
        }
        // Try to read block and check stats
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore, refs[i].addr);
        if (status == AKU_EUNAVAILABLE) {
            // block was deleted due to retention.
            Logger::msg(AKU_LOG_INFO, "Block " + std::to_string(refs[i].addr));
        } else if (status == AKU_SUCCESS) {
            SubtreeRef out = INIT_SUBTREE_REF;
            const SubtreeRef* iref = reinterpret_cast<const SubtreeRef*>(block->get_cdata());
            if (iref->type == NBTreeBlockType::LEAF) {
                NBTreeLeaf leaf(block);
                status = init_subtree_from_leaf(leaf, out);
                if (status != AKU_SUCCESS) {
                    AKU_PANIC("Can't summarize leaf node at " + std::to_string(refs[i].addr) + " error: "
                                                              + StatusUtil::str(status));
                }
            } else {
                NBTreeSuperblock superblock(block);
                status = init_subtree_from_subtree(superblock, out);
                if (status != AKU_SUCCESS) {
                    AKU_PANIC("Can't summarize inner node at " + std::to_string(refs[i].addr) + " error: "
                                                               + StatusUtil::str(status));
                }
            }
            // Compare metadata refs
            std::stringstream fmt;
            int nbadfields = 0;
            if (refs[i].begin != out.begin) {
                fmt << ".begin " << refs[i].begin << " != " << out.begin << "; ";
                nbadfields++;
            }
            if (refs[i].end != out.end) {
                fmt << ".end " << refs[i].end << " != " << out.end << "; ";
                nbadfields++;
            }
            if (refs[i].count != out.count) {
                fmt << ".count " << refs[i].count << " != " << out.count << "; ";
                nbadfields++;
            }
            if (refs[i].id != out.id) {
                fmt << ".id " << refs[i].id << " != " << out.id << "; ";
                nbadfields++;
            }
            if (!same_value(refs[i].max, out.max)) {
                fmt << ".max " << refs[i].max << " != " << out.max << "; ";
                nbadfields++;
            }
            if (!same_value(refs[i].min, out.min)) {
                fmt << ".min " << refs[i].min << " != " << out.min << "; ";
                nbadfields++;
            }
            if (!same_value(refs[i].sum, out.sum)) {
                fmt << ".sum " << refs[i].sum << " != " << out.sum << "; ";
                nbadfields++;
            }
            if (refs[i].version != out.version) {
                fmt << ".version " << refs[i].version << " != " << out.version << "; ";
                nbadfields++;
            }
            if (nbadfields) {
                Logger::msg(AKU_LOG_ERROR, "Inner node contains bad values: " + fmt.str());
                nerrors++;
            } else {
                nodes2follow.push_back(refs[i].addr);
            }
        } else {
            // Some other error occured.
            AKU_PANIC("Can't read node from block-store: " + StatusUtil::str(status));
        }
    }
    if (nerrors) {
        AKU_PANIC("Invalid structure at " + std::to_string(required_level) + " examine log for more details.");
    }

    // Recur
    for (auto addr: nodes2follow) {
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore, addr);
        const SubtreeRef* iref = reinterpret_cast<const SubtreeRef*>(block->get_cdata());
        if (iref->type == NBTreeBlockType::INNER) {
            NBTreeSuperblock child(addr, bstore);
            // We need to check backrefs only on top level that is used for crash recovery.
            // In all other levels backreferences is not used for anything.
            check_superblock_consistency(bstore, &child, required_level == 0 ? 0 : required_level - 1, false);
        }
    }
}


void NBTreeExtent::check_extent(NBTreeExtent const* extent, std::shared_ptr<BlockStore> bstore, size_t level) {
    if (level == 0) {
        // Leaf node
        return;
    }
    auto subtree = dynamic_cast<NBTreeSBlockExtent const*>(extent);
    if (subtree) {
        // Complex extent.
        auto const* curr = subtree->curr_.get();
        check_superblock_consistency(bstore, curr, static_cast<u16>(level - 1), true);
    }
}

// ///////////////////// //
//   NBTreeExtentsList   //
// ///////////////////// //


NBTreeExtentsList::NBTreeExtentsList(aku_ParamId id, std::vector<LogicAddr> addresses, std::shared_ptr<BlockStore> bstore)
    : bstore_(bstore)
    , id_(id)
    , last_(0ull)
    , rescue_points_(std::move(addresses))
    , initialized_(false)
    , write_count_(0ul)
#ifdef AKU_ENABLE_MUTATION_TESTING
    , rd_()
    , rand_gen_(rd_())
    , dist_(0, 1000)
    , threshold_(1)
#endif
{
    if (rescue_points_.size() >= std::numeric_limits<u16>::max()) {
        AKU_PANIC("Tree depth is too large");
    }
}

std::tuple<size_t, size_t> NBTreeExtentsList::bytes_used() const {
    size_t c1 = 0, c2 = 0;
    if (!extents_.empty()) {
        auto leaf = dynamic_cast<NBTreeLeafExtent const*>(extents_.front().get());
        if (leaf != nullptr) {
            c1 = leaf->bytes_used();
        }
        if (extents_.size() > 1) {
            c2 = 0x1000 * (extents_.size() - 1);
        }
    }
    return std::make_tuple(c1, c2);
}

void NBTreeExtentsList::force_init() {
    UniqueLock lock(lock_);
    if (!initialized_) {
        init();
    }
}

size_t NBTreeExtentsList::_get_uncommitted_size() const {
    if (!extents_.empty()) {
        auto leaf = dynamic_cast<NBTreeLeafExtent const*>(extents_.front().get());
        if (leaf == nullptr) {
            // Small check to make coverity scan happy
            AKU_PANIC("Bad extent at level 0, leaf node expected");
        }
        return leaf->leaf_->_get_uncommitted_size();
    }
    return 0;
}

bool NBTreeExtentsList::is_initialized() const {
    SharedLock lock(lock_);
    return initialized_;
}

std::vector<NBTreeExtent const*> NBTreeExtentsList::get_extents() const {
    // NOTE: no lock here because we're returning extents and this breaks
    //       all thread safety but this is doesn't matter because this method
    //       should be used only for testing in single-threaded setting.
    std::vector<NBTreeExtent const*> result;
    for (auto const& ptr: extents_) {
        result.push_back(ptr.get());
    }
    return result;
}

#ifdef AKU_ENABLE_MUTATION_TESTING
u32 NBTreeExtentsList::chose_random_node() {
    std::uniform_int_distribution<u32> rext(0, static_cast<u32>(extents_.size()-1));
    u32 ixnode = rext(rand_gen_);
    return ixnode;
}
#endif

std::tuple<aku_Status, AggregationResult> NBTreeExtentsList::get_aggregates(u32 ixnode) const {
    auto it = extents_.at(ixnode)->aggregate(0, AKU_MAX_TIMESTAMP);
    aku_Timestamp ts;
    AggregationResult dest;
    size_t outsz;
    aku_Status status;
    std::tie(status, outsz) = it->read(&ts, &dest, 1);
    if (outsz == 0) {
        Logger::msg(AKU_LOG_ERROR, "Can't split the node: no data returned from aggregate query");
        return std::make_tuple(AKU_ENOT_FOUND, dest);
    }
    if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
        Logger::msg(AKU_LOG_ERROR, "Can't split the node: " + StatusUtil::str(status));
    }
    return std::make_tuple(AKU_SUCCESS, dest);
}

#ifdef AKU_ENABLE_MUTATION_TESTING
LogicAddr NBTreeExtentsList::split_random_node(u32 ixnode) {
    AggregationResult dest;
    aku_Status status;
    std::tie(status, dest) = get_aggregates(ixnode);
    if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
        return EMPTY_ADDR;
    }
    aku_Timestamp begin = dest._begin;
    aku_Timestamp end   = dest._end;

    // Chose the pivot point
    std::uniform_int_distribution<aku_Timestamp> rsplit(begin, end);
    aku_Timestamp pivot = rsplit(rand_gen_);
    LogicAddr addr;
    bool parent_saved;
    if (extents_.at(ixnode)->is_dirty()) {
        std::tie(parent_saved, addr) = extents_.at(ixnode)->split(pivot);
        return addr;
    }
    return EMPTY_ADDR;
}
#endif

void NBTreeExtentsList::check_rescue_points(u32 i) const {
    if (i == 0) {
        return;
    }
    // we can use i-1 value to restore the i'th
    LogicAddr addr = rescue_points_.at(i-1);

    auto aggit = extents_.at(i)->aggregate(AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP);
    aku_Timestamp ts;
    AggregationResult res;
    size_t sz;
    aku_Status status;
    std::tie(status, sz) = aggit->read(&ts, &res, 1);
    if (sz == 0 || (status != AKU_SUCCESS && status != AKU_ENO_DATA)) {
        // Failed check
        assert(false);
    }

    NBTreeSuperblock sblock(id_, EMPTY_ADDR, 0, 0);
    std::vector<SubtreeRef> refs;
    while(addr != EMPTY_ADDR) {
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, addr);
        if (status == AKU_EUNAVAILABLE) {
            // Block removed due to retention. Can't actually check anything.
            return;
        }
        const SubtreeRef* ref = subtree_cast(block->get_cdata());
        SubtreeRef tmp = *ref;
        tmp.addr = addr;
        refs.push_back(tmp);
        addr = ref->addr;
    }
    for(auto it = refs.rbegin(); it < refs.rend(); it++) {
        status = sblock.append(*it);
        assert(status == AKU_SUCCESS);
    }
    aggit = sblock.aggregate(AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP, bstore_);
    AggregationResult newres;
    std::tie(status, sz) = aggit->read(&ts, &newres, 1);
    if (sz == 0 || (status != AKU_SUCCESS && status != AKU_ENO_DATA)) {
        // Failed check
        assert(false);
    }
    assert(res._begin   == newres._begin);
    assert(res._end     == newres._end);
    assert(res.cnt      == newres.cnt);
    assert(res.first    == newres.first);
    assert(res.last     == newres.last);
    assert(res.max      == newres.max);
    assert(res.min      == newres.min);
    assert(res.maxts    == newres.maxts);
    assert(res.mints    == newres.mints);
}

std::tuple<aku_Status, LogicAddr> NBTreeExtentsList::_split(aku_Timestamp pivot) {
    aku_Status status;
    LogicAddr paddr = EMPTY_ADDR;
    size_t extent_index = extents_.size();
    // Find the extent that contains the pivot
    for (size_t i = 0; i < extents_.size(); i++) {
        auto it = extents_.at(i)->aggregate(AKU_MIN_TIMESTAMP, AKU_MAX_TIMESTAMP);
        AggregationResult res;
        size_t outsz;
        aku_Timestamp ts;
        std::tie(status, outsz) = it->read(&ts, &res, 1);
        if (status == AKU_SUCCESS) {
            if (res._begin <= pivot && pivot < res._end) {
                extent_index = i;
            }
            break;
        } else if (status == AKU_ENO_DATA || status == AKU_EUNAVAILABLE) {
            continue;
        }
        return std::make_tuple(status, paddr);
    }
    if (extent_index == extents_.size()) {
        return std::make_tuple(AKU_ENOT_FOUND, paddr);
    }
    bool parent_saved = false;
    std::tie(parent_saved, paddr) = extents_.at(extent_index)->split(pivot);
    if (paddr != EMPTY_ADDR) {
        std::shared_ptr<Block> rblock;
        std::tie(status, rblock) = read_and_check(bstore_, paddr);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't read @" + std::to_string(paddr) + ", error: " + StatusUtil::str(status));
            AKU_PANIC("Can't read back the data");
        }
        // extent_index and the level of the node can mismatch
        auto pnode = subtree_cast(rblock->get_cdata());
        if (rescue_points_.size() > pnode->level) {
            rescue_points_.at(pnode->level) = paddr;
        } else {
            rescue_points_.push_back(paddr);
        }
        if (extent_index > 0) {
            u16 prev_fanout = 0;
            LogicAddr prev_addr = EMPTY_ADDR;
            if (pnode->fanout_index < AKU_NBTREE_MAX_FANOUT_INDEX) {
                prev_fanout = pnode->fanout_index + 1;
                prev_addr   = paddr;
            }
            auto prev_extent = extent_index - 1;
            status = extents_.at(prev_extent)->update_prev_addr(prev_addr);
            if (status != AKU_SUCCESS) {
                AKU_PANIC("Invalid access pattern in split method");
            }
            status = extents_.at(prev_extent)->update_fanout_index(prev_fanout);
            if (status != AKU_SUCCESS) {
                AKU_PANIC("Can't update fanout index of the node");
            }
        }
    }
    return std::make_tuple(status, paddr);
}

NBTreeAppendResult NBTreeExtentsList::append(aku_Timestamp ts, double value, bool allow_duplicate_timestamps) {
    UniqueLock lock(lock_);  // NOTE: NBTreeExtentsList::append(subtree) can be called from here
                             //       recursively (maybe even many times).
    if (!initialized_) {
        init();
    }
    if (allow_duplicate_timestamps ? ts < last_ : ts <= last_) {
        return NBTreeAppendResult::FAIL_LATE_WRITE;
    }
    last_ = ts;
    write_count_++;
    if (extents_.size() == 0) {
        // create first leaf node
        std::unique_ptr<NBTreeExtent> leaf;
        leaf.reset(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, EMPTY_ADDR));
        extents_.push_back(std::move(leaf));
        rescue_points_.push_back(EMPTY_ADDR);
    }
    auto result = NBTreeAppendResult::OK;
    bool parent_saved = false;
    LogicAddr addr = EMPTY_ADDR;
    std::tie(parent_saved, addr) = extents_.front()->append(ts, value);
    if (addr != EMPTY_ADDR) {
        // We need to clear the rescue point since the address is already
        // persisted.
        //addr = parent_saved ? EMPTY_ADDR : addr;
        if (rescue_points_.size() > 0) {
            rescue_points_.at(0) = addr;
        } else {
            rescue_points_.push_back(addr);
        }
        result = NBTreeAppendResult::OK_FLUSH_NEEDED;
    }
    return result;
}

bool NBTreeExtentsList::append(const SubtreeRef &pl) {
    // NOTE: this method should be called by extents which
    //       is called by another `append` overload recursively
    //       and lock will be held already so no lock here!
    u16 lvl = static_cast<u16>(pl.level + 1);
    NBTreeExtent* root = nullptr;
    if (extents_.size() > lvl) {
        // Fast path
        root = extents_[lvl].get();
    } else if (extents_.size() == lvl) {
        std::unique_ptr<NBTreeExtent> p;
        p.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(),
                                       id_,
                                       EMPTY_ADDR,
                                       lvl));
        root = p.get();
        extents_.push_back(std::move(p));
        rescue_points_.push_back(EMPTY_ADDR);
    } else {
        Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Invalid node level - " + std::to_string(lvl));
        AKU_PANIC("Invalid node level");
    }
    bool parent_saved = false;
    LogicAddr addr = EMPTY_ADDR;
    write_count_++;
    std::tie(parent_saved, addr) = root->append(pl);
    if (addr != EMPTY_ADDR) {
        // NOTE: `addr != EMPTY_ADDR` means that something was saved to disk (current node or parent node).
        //addr = parent_saved ? EMPTY_ADDR : addr;
        if (rescue_points_.size() > lvl) {
            rescue_points_.at(lvl) = addr;
        } else if (rescue_points_.size() == lvl) {
            rescue_points_.push_back(addr);
        } else {
            // INVARIANT: order of commits - leaf node committed first, then inner node at level 1,
            // then level 2 and so on. Address of the inner node (or root node) should be greater then addresses
            // of all its children.
            AKU_PANIC("Out of order commit!");
        }
        return true;
    }
    return false;
}

void NBTreeExtentsList::open() {
    // NOTE: lock doesn't needed here because this method will be called by
    //       the `force_init` method that already holds the write lock.
    Logger::msg(AKU_LOG_INFO, std::to_string(id_) + " Trying to open tree, repair status - OK, addr: " +
                              std::to_string(rescue_points_.back()));
    // NOTE: rescue_points_ list should have at least two elements [EMPTY_ADDR, Root].
    // Because of this `addr` is always an inner node.
    if (rescue_points_.size() < 2) {
        // Only one page was saved to disk!
        // Create new root, because now we will create new root (this is the only case
        // when new root will be created during tree-open process).
        u16 root_level = 1;
        std::unique_ptr<NBTreeSBlockExtent> root_extent;
        root_extent.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(), id_, EMPTY_ADDR, root_level));

        // Read old leaf node. Add single element to the root.
        LogicAddr addr = rescue_points_.front();
        std::shared_ptr<Block> leaf_block;
        aku_Status status;
        std::tie(status, leaf_block) = read_and_check(bstore_, addr);
        if (status != AKU_SUCCESS) {
            // Tree is old and should be removed, no data was left on the block device.
            Logger::msg(AKU_LOG_INFO, std::to_string(id_) + " Dead tree detected");
            std::unique_ptr<NBTreeExtent> leaf_extent(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, EMPTY_ADDR));
            extents_.push_back(std::move(leaf_extent));
            initialized_ = true;
            return;
        }
        NBTreeLeaf leaf(leaf_block);  // fully loaded leaf
        SubtreeRef sref = INIT_SUBTREE_REF;
        status = init_subtree_from_leaf(leaf, sref);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't open tree at: " + std::to_string(addr) +
                        " error: " + StatusUtil::str(status));
            AKU_PANIC("Can't open tree");
        }
        sref.addr = addr;
        root_extent->append(sref);  // this always should return `false` and `EMPTY_ADDR`, no need to check this.

        // Create new empty leaf
        std::unique_ptr<NBTreeExtent> leaf_extent(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, addr));
        extents_.push_back(std::move(leaf_extent));
        extents_.push_back(std::move(root_extent));
        rescue_points_.push_back(EMPTY_ADDR);
    } else {
        // Initialize root node.
        auto root_level = rescue_points_.size() - 1;
        LogicAddr addr = rescue_points_.back();
        std::unique_ptr<NBTreeSBlockExtent> root;
        // CoW should be used here, otherwise tree height will increase after each reopen.
        root.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(), id_, addr, static_cast<u16>(root_level)));

        // Initialize leaf using new leaf node!
        // TODO: leaf_prev = load_prev_leaf_addr(root);
        LogicAddr leaf_prev = EMPTY_ADDR;
        std::unique_ptr<NBTreeExtent> leaf(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, leaf_prev));
        extents_.push_back(std::move(leaf));

        // Initialize inner nodes.
        for (size_t i = 1; i < root_level; i++) {
            // TODO: leaf_prev = load_prev_inner_addr(root, i);
            LogicAddr inner_prev = EMPTY_ADDR;
            std::unique_ptr<NBTreeExtent> inner;
            inner.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(),
                                               id_, inner_prev, static_cast<u16>(i)));
            extents_.push_back(std::move(inner));
        }

        extents_.push_back(std::move(root));
    }
    // Scan extents backwards and remove stalled extents
    int ext2remove = 0;
    for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
        if ((*it)->status() == NBTreeExtent::ExtentStatus::KILLED_BY_RETENTION) {
            ext2remove++;
        } else {
            break;
        }
    }
    // All extents can't be removed because leaf extent can't return `KILLED_BY_RETENTION` status.
    // If all data was deleted by retention, only one new leaf node will be present in the `extents_`
    // list.
    for(;ext2remove --> 0;) {
        extents_.pop_back();
        rescue_points_.pop_back();
    }

    // Restore `last_`
    if (extents_.size()) {
        auto it = extents_.back()->search(AKU_MAX_TIMESTAMP, 0);
        aku_Timestamp ts;
        double val;
        aku_Status status;
        size_t nread;
        std::tie(status, nread) = it->read(&ts, &val, 1);
        if (status != AKU_SUCCESS) {
            // The tree is empty due to retention so we can use smallest possible
            // timestamp
            Logger::msg(AKU_LOG_TRACE, "Tree " + std::to_string(this->id_) + " is empty due to retention");
            ts = AKU_MIN_TIMESTAMP;
        }
        last_ = ts;
    }
    assert(rescue_points_.size() == extents_.size());
}

static void create_empty_extents(std::shared_ptr<NBTreeExtentsList> self,
                                 std::shared_ptr<BlockStore> bstore,
                                 aku_ParamId id,
                                 size_t nlevels,
                                 std::vector<std::unique_ptr<NBTreeExtent>>* extents)
{
    for (size_t i = 0; i < nlevels; i++) {
        if (i == 0) {
            // Create empty leaf node
            std::unique_ptr<NBTreeLeafExtent> leaf;
            leaf.reset(new NBTreeLeafExtent(bstore, self, id, EMPTY_ADDR));
            extents->push_back(std::move(leaf));
        } else {
            // Create empty inner node
            std::unique_ptr<NBTreeSBlockExtent> inner;
            u16 level = static_cast<u16>(i);
            inner.reset(new NBTreeSBlockExtent(bstore, self, id, EMPTY_ADDR, level));
            extents->push_back(std::move(inner));
        }
    }
}

void NBTreeExtentsList::repair() {
    // NOTE: lock doesn't needed for the same reason as in `open` method.
    Logger::msg(AKU_LOG_INFO, std::to_string(id_) + " Trying to open tree, repair status - REPAIR, addr: " +
                              std::to_string(rescue_points_.back()));
    create_empty_extents(shared_from_this(), bstore_, id_, rescue_points_.size(), &extents_);
    std::stack<LogicAddr> stack;
    // Follow rescue points in backward direction
    for (auto addr: rescue_points_) {
        stack.push(addr);
    }
    std::vector<SubtreeRef> refs;
    refs.reserve(32);
    while (!stack.empty()) {
        LogicAddr curr = stack.top();
        stack.pop();
        if (curr == EMPTY_ADDR) {
            // Insert all nodes in direct order
            for(auto it = refs.rbegin(); it < refs.rend(); it++) {
                append(*it);  // There is no need to check return value.
            }
            refs.clear();
            // Invariant: this part is guaranteed to be called for every level that have
            // the data to recover. First child node of every extent will have `get_prev_addr()`
            // return EMPTY_ADDR. This value will trigger this code.
            continue;
        }
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, curr);
        if (status != AKU_SUCCESS) {
            // Stop collecting data and force building of the current extent.
            stack.push(EMPTY_ADDR);
            // The node was deleted because of retention process we should
            continue;  // with the next rescue point which may be newer.
        }
        const SubtreeRef* curr_pref = reinterpret_cast<const SubtreeRef*>(block->get_cdata());
        if (curr_pref->type == NBTreeBlockType::LEAF) {
            NBTreeLeaf leaf(block);
            SubtreeRef ref = INIT_SUBTREE_REF;
            status = init_subtree_from_leaf(leaf, ref);
            if (status != AKU_SUCCESS) {
                Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't summarize leaf node at " +
                                           std::to_string(curr) + " error: " +
                                           StatusUtil::str(status));
            }
            ref.addr = curr;
            refs.push_back(ref);
            stack.push(leaf.get_prev_addr());  // get_prev_addr() can return EMPTY_ADDR
        } else {
            NBTreeSuperblock sblock(block);
            SubtreeRef ref = INIT_SUBTREE_REF;
            status = init_subtree_from_subtree(sblock, ref);
            if (status != AKU_SUCCESS) {
                Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't summarize inner node at " +
                                           std::to_string(curr) + " error: " +
                                           StatusUtil::str(status));
            }
            ref.addr = curr;
            refs.push_back(ref);
            stack.push(sblock.get_prev_addr());  // get_prev_addr() can return EMPTY_ADDR
        }
    }
}

void NBTreeExtentsList::init() {
    initialized_ = true;
    if (rescue_points_.empty() == false) {
        auto rstat = repair_status(rescue_points_);
        // Tree should be opened normally.
        if (rstat == RepairStatus::OK) {
            open();
        }
        // Tree should be restored (crush recovery kicks in here).
        else {
            repair();
        }
    }
}

std::unique_ptr<RealValuedOperator> NBTreeExtentsList::search(aku_Timestamp begin, aku_Timestamp end) const {
    if (!initialized_) {
        const_cast<NBTreeExtentsList*>(this)->force_init();
    }
    SharedLock lock(lock_);
    std::vector<std::unique_ptr<RealValuedOperator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back((*it)->search(begin, end));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(root->search(begin, end));
        }
    }
    if (iterators.size() == 1) {
        return std::move(iterators.front());
    }
    std::unique_ptr<RealValuedOperator> concat;
    concat.reset(new ChainOperator(std::move(iterators)));
    return concat;
}

std::unique_ptr<RealValuedOperator> NBTreeExtentsList::filter(aku_Timestamp begin,
                                                              aku_Timestamp end,
                                                              const ValueFilter& filter) const
{
    if (!initialized_) {
        const_cast<NBTreeExtentsList*>(this)->force_init();
    }
    SharedLock lock(lock_);
    std::vector<std::unique_ptr<RealValuedOperator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back((*it)->filter(begin, end, filter));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(root->filter(begin, end, filter));
        }
    }
    if (iterators.size() == 1) {
        return std::move(iterators.front());
    }
    std::unique_ptr<RealValuedOperator> concat;
    concat.reset(new ChainOperator(std::move(iterators)));
    return concat;
}

std::unique_ptr<AggregateOperator> NBTreeExtentsList::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    if (!initialized_) {
        const_cast<NBTreeExtentsList*>(this)->force_init();
    }
    SharedLock lock(lock_);
    std::vector<std::unique_ptr<AggregateOperator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back((*it)->aggregate(begin, end));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(root->aggregate(begin, end));
        }
    }
    if (iterators.size() == 1) {
        return std::move(iterators.front());
    }
    std::unique_ptr<AggregateOperator> concat;
    concat.reset(new CombineAggregateOperator(std::move(iterators)));
    return concat;

}

std::unique_ptr<AggregateOperator> NBTreeExtentsList::group_aggregate(aku_Timestamp begin, aku_Timestamp end, aku_Timestamp step) const {
    if (!initialized_) {
        const_cast<NBTreeExtentsList*>(this)->force_init();
    }
    SharedLock lock(lock_);
    std::vector<std::unique_ptr<AggregateOperator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back((*it)->group_aggregate(begin, end, step));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(root->group_aggregate(begin, end, step));
        }
    }
    std::unique_ptr<AggregateOperator> concat;
    concat.reset(new CombineGroupAggregateOperator(begin, end, step, std::move(iterators)));
    return concat;
}

std::unique_ptr<AggregateOperator> NBTreeExtentsList::group_aggregate_filter(aku_Timestamp begin,
                                                                             aku_Timestamp end,
                                                                             aku_Timestamp step,
                                                                             const AggregateFilter &filter) const
{
    auto iter = group_aggregate(begin, end, step);
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeGroupAggregateFilter(filter, std::move(iter)));
    return result;
}

std::unique_ptr<AggregateOperator> NBTreeExtentsList::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    if (!initialized_) {
        const_cast<NBTreeExtentsList*>(this)->force_init();
    }
    SharedLock lock(lock_);
    std::vector<std::unique_ptr<AggregateOperator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back((*it)->candlesticks(begin, end, hint));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(root->candlesticks(begin, end, hint));
        }
    }
    if (iterators.size() == 1) {
        return std::move(iterators.front());
    }
    std::unique_ptr<AggregateOperator> concat;
    // NOTE: there is no intersections between extents so we can join iterators
    concat.reset(new CombineAggregateOperator(std::move(iterators)));
    return concat;
}


std::vector<LogicAddr> NBTreeExtentsList::close() {
    UniqueLock lock(lock_);
    if (initialized_) {
        if (write_count_) {
            Logger::msg(AKU_LOG_TRACE, std::to_string(id_) + " Going to close the tree.");
            LogicAddr addr = EMPTY_ADDR;
            bool parent_saved = false;
            for(size_t index = 0ul; index < extents_.size(); index++) {
                if (extents_.at(index)->is_dirty()) {
                    std::tie(parent_saved, addr) = extents_.at(index)->commit(true);
                }
            }
            assert(!parent_saved);
            // NOTE: at this point `addr` should contain address of the tree's root.
            std::vector<LogicAddr> result(rescue_points_.size(), EMPTY_ADDR);
            result.back() = addr;
            std::swap(rescue_points_, result);
        } else {
            // Special case, tree was opened but left unmodified
            if (rescue_points_.size() == 2 && rescue_points_.back() == EMPTY_ADDR) {
                rescue_points_.pop_back();
            }
        }
    }
    #ifdef AKU_UNIT_TEST_CONTEXT
    // This code should be executed only from unit-test.
    if (extents_.size() > 1) {
        NBTreeExtent::check_extent(extents_.back().get(), bstore_, extents_.size() - 1);
    }
    #endif
    // This node is not initialized now but can be restored from `rescue_points_` list.
    extents_.clear();
    initialized_ = false;
    // roots should be a list of EMPTY_ADDR values followed by
    // the address of the root node [E, E, E.., rootaddr].
    return rescue_points_;
}

std::vector<LogicAddr> NBTreeExtentsList::get_roots() const {
    SharedLock lock(lock_);
    return rescue_points_;
}

std::vector<LogicAddr> NBTreeExtentsList::_get_roots() const {
    return rescue_points_;
}

NBTreeExtentsList::RepairStatus NBTreeExtentsList::repair_status(std::vector<LogicAddr> const& rescue_points) {
    ssize_t count = static_cast<ssize_t>(rescue_points.size()) -
                    std::count(rescue_points.begin(), rescue_points.end(), EMPTY_ADDR);
    if (count == 1 && rescue_points.back() != EMPTY_ADDR) {
        return RepairStatus::OK;
    }
    return RepairStatus::REPAIR;
}


static NBTreeBlockType _dbg_get_block_type(std::shared_ptr<Block> block) {
    auto ref = reinterpret_cast<SubtreeRef const*>(block->get_cdata());
    return ref->level == 0 ? NBTreeBlockType::LEAF : NBTreeBlockType::INNER;
}

void NBTreeExtentsList::debug_print(LogicAddr root, std::shared_ptr<BlockStore> bstore, size_t depth) {
    std::string pad(depth, ' ');
    if (root == EMPTY_ADDR) {
        std::cout << pad << "EMPTY_ADDR" << std::endl;
        return;
    }
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = read_and_check(bstore, root);
    if (status != AKU_SUCCESS) {
        std::cout << pad << "ERROR: Can't read block at " << root << " " << StatusUtil::str(status) << std::endl;
    }
    auto type = _dbg_get_block_type(block);
    if (type == NBTreeBlockType::LEAF) {
        NBTreeLeaf leaf(block);
        std::vector<aku_Timestamp> ts;
        std::vector<double> xs;
        status = leaf.read_all(&ts, &xs);
        if (status != AKU_SUCCESS) {
            std::cout << pad << "ERROR: Can't decompress block at " << root << " " << StatusUtil::str(status) << std::endl;
        }
        std::cout << pad << "Leaf at " << root << " TS: [" << ts.front() << ", " << ts.back() << "]" << std::endl;
        std::cout << pad << "        " << root << " XS: [" << ts.front() << ", " << ts.back() << "]" << std::endl;
    } else {
        NBTreeSuperblock inner(root, bstore);
        std::vector<SubtreeRef> refs;
        status = inner.read_all(&refs);
        if (status != AKU_SUCCESS) {
            std::cout << pad << "ERROR: Can't decompress superblock at " << root << " " << StatusUtil::str(status) << std::endl;
        }
        std::cout << pad << "Node at " << root << " TS: [" << refs.front().begin << ", " << refs.back().end << "]" << std::endl;
        for (SubtreeRef ref: refs) {
            std::cout << pad << "- node: " << ref.addr << std::endl;
            std::cout << pad << "- TS: [" << ref.begin << ", " << ref.end << "]" << std::endl;
            std::cout << pad << "- level: " << ref.level << std::endl;
            std::cout << pad << "- fanout index: " << ref.fanout_index << std::endl;
            debug_print(ref.addr, bstore, depth + 4);
        }
    }
}

}}
