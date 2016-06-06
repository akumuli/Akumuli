// C++
#include <algorithm>
#include <vector>
#include <sstream>

// Boost
#include <boost/scope_exit.hpp>

// App
#include "nbtree.h"
#include "akumuli_version.h"
#include "status_util.h"
#include "log_iface.h"

/** NOTE:
  * BlockStore should have cache. This cache should be implemented on
  * _this_ level because importance of each block should be taken into
  * account. Eviction algorithm should check whether or not block was
  * actually deleted. Alg. for this:
  * 1. Get weak ptr for the block.
  * 2. Remove block from cache.
  * 3. Try to lock weak ptr. On success try to remove another block.
  * 4. Otherwise we done.
  */


namespace Akumuli {
namespace StorageEngine {

static SubtreeRef* subtree_cast(u8* p) {
    return reinterpret_cast<SubtreeRef*>(p);
}

static SubtreeRef const* subtree_cast(u8 const* p) {
    return reinterpret_cast<SubtreeRef const*>(p);
}

//! Initialize object from leaf node
static aku_Status init_subtree_from_leaf(const NBTreeLeaf& leaf, SubtreeRef& out) {
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
    out.max = max;
    out.min = min;
    out.sum = sum;
    out.begin = ts.front();
    out.end = ts.back();
    out.count = xs.size();
    // Set node's data
    out.id = leaf.get_id();
    out.level = 0;
    out.version = AKUMULI_VERSION;
    out.fanout_index = leaf.get_fanout();
    out.payload_size = 0;
    return AKU_SUCCESS;
}

static aku_Status init_subtree_from_subtree(const NBTreeSuperblock& node, SubtreeRef& backref) {
    std::vector<SubtreeRef> refs;
    aku_Status status = node.read_all(&refs);
    if (status != AKU_SUCCESS) {
        return status;
    }
    backref.begin = refs.front().begin;
    backref.end = refs.back().end;
    backref.count = 0;
    backref.sum = 0;

    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    for (const SubtreeRef& sref: refs) {
        backref.count += sref.count;
        backref.sum   += sref.sum;
        min = std::min(min, sref.min);
        max = std::max(max, sref.max);
    }
    backref.min = min;
    backref.max = max;

    // Node level information
    backref.id = node.get_id();
    backref.level = node.get_level();
    backref.version = AKUMULI_VERSION;
    backref.fanout_index = node.get_fanout();
    backref.payload_size = 0;
    return AKU_SUCCESS;
}


/** NBTreeIterator implementation for leaf node.
  * This is very basic. All node's data is copied to
  * the internal buffer by c-tor.
  */
struct NBTreeLeafIterator : NBTreeIterator {

    // TODO: Leaf iterator should be lazy!

    //! Starting timestamp
    aku_Timestamp              begin_;
    //! Final timestamp
    aku_Timestamp              end_;
    //! Timestamps
    std::vector<aku_Timestamp> tsbuf_;
    //! Values
    std::vector<double>        xsbuf_;
    //! Range begin
    ssize_t                    from_;
    //! Range end
    ssize_t                    to_;
    //! Status of the iterator initialization process
    aku_Status                 status_;

    NBTreeLeafIterator(aku_Status status)
        : begin_()
        , end_()
        , from_()
        , to_()
        , status_(status)
    {
    }

    NBTreeLeafIterator(aku_Timestamp begin, aku_Timestamp end, NBTreeLeaf const& node)
        : begin_(begin)
        , end_(end)
        , from_()
        , to_()
    {
        aku_Timestamp min = std::min(begin, end);
        aku_Timestamp max = std::max(begin, end);
        aku_Timestamp nb, ne;
        std::tie(nb, ne) = node.get_timestamps();
        if (max < nb || ne < min) {
            status_ = AKU_ENO_DATA;
            return;
        }
        status_ = node.read_all(&tsbuf_, &xsbuf_);
        if (status_ == AKU_SUCCESS) {
            if (begin < end) {
                // FWD direction
                auto it_begin = std::lower_bound(tsbuf_.begin(), tsbuf_.end(), begin_);
                if (it_begin != tsbuf_.end()) {
                    from_ = std::distance(tsbuf_.begin(), it_begin);
                } else {
                    from_ = 0;
                    assert(tsbuf_.front() > begin);
                }
                auto it_end = std::lower_bound(tsbuf_.begin(), tsbuf_.end(), end_);
                to_ = std::distance(tsbuf_.begin(), it_end);
            } else {
                // BWD direction
                auto it_begin = std::upper_bound(tsbuf_.begin(), tsbuf_.end(), begin_);
                from_ = std::distance(it_begin, tsbuf_.end());

                auto it_end = std::upper_bound(tsbuf_.begin(), tsbuf_.end(), end_);
                to_ = std::distance(it_end, tsbuf_.end());
                std::reverse(tsbuf_.begin(), tsbuf_.end());
                std::reverse(xsbuf_.begin(), xsbuf_.end());
            }
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size);
    virtual Direction get_direction();
};


std::tuple<aku_Status, size_t> NBTreeLeafIterator::read(aku_Timestamp *destts, double *destval, size_t size) {
    if (status_ != AKU_SUCCESS) {
        return std::make_tuple(status_, 0);
    }
    size_t toread = to_ - from_;
    if (toread > size) {
        toread = size;
    }
    if (toread == 0) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }
    auto begin = from_;
    auto end = from_ + toread;
    std::copy(tsbuf_.begin() + begin, tsbuf_.begin() + end, destts);
    std::copy(xsbuf_.begin() + begin, xsbuf_.begin() + end, destval);
    from_ += toread;
    return std::make_tuple(AKU_SUCCESS, toread);
}

NBTreeIterator::Direction NBTreeLeafIterator::get_direction() {
    if (begin_ < end_) {
        return Direction::FORWARD;
    }
    return Direction::BACKWARD;
}

// ////////////////////////////// //
//  NBTreeIterator concatenation  //
// ////////////////////////////// //

/** Concatenating iterator.
  * Accepts list of iterators in the c-tor. All iterators then
  * can be seen as one iterator. Iterators should be in correct
  * order.
  */
struct IteratorConcat : NBTreeIterator {
    typedef std::vector<std::unique_ptr<NBTreeIterator>> IterVec;
    IterVec   iter_;
    Direction dir_;
    u32       iter_index_;

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    IteratorConcat(TVec&& iter)
        : iter_(std::forward<TVec>(iter))
        , iter_index_(0)
    {
        if (iter_.empty()) {
            dir_ = Direction::FORWARD;
        } else {
            dir_ = iter_.front()->get_direction();
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size);
    virtual Direction get_direction();
};

std::tuple<aku_Status, size_t> IteratorConcat::read(aku_Timestamp *destts, double *destval, size_t size) {
    aku_Status status = AKU_SUCCESS;
    size_t ressz = 0;  // current size
    size_t accsz = 0;  // accumulated size
    while(iter_index_ < iter_.size()) {
        std::tie(status, ressz) = iter_[iter_index_]->read(destts, destval, size);
        destts += ressz;
        destval += ressz;
        size -= ressz;
        accsz += ressz;
        if (size == 0) {
            break;
        }
        iter_index_++;
        if (status == AKU_ENO_DATA) {
            // this leaf node is empty, continue with next
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Stop iteration or error!
            return std::tie(status, accsz);
        }
    }
    return std::tie(status, accsz);
}

NBTreeIterator::Direction IteratorConcat::get_direction() {
    return dir_;
}

// //////////////// //
//    NBTreeLeaf    //
// //////////////// //

NBTreeLeaf::NBTreeLeaf(aku_ParamId id, LogicAddr prev, u16 fanout_index)
    : prev_(prev)
    , buffer_(AKU_BLOCK_SIZE, 0)
    , writer_(id, buffer_.data() + sizeof(SubtreeRef), AKU_BLOCK_SIZE - sizeof(SubtreeRef))
    , fanout_index_(fanout_index)
{
    SubtreeRef* subtree = subtree_cast(buffer_.data());
    subtree->addr = prev;
    subtree->level = 0;  // Leaf node
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
}


//! Read block from blockstoroe.
static std::shared_ptr<Block> read_block_from_bstore(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can't read block - " + StatusUtil::str(status));
    }
    return block;
}

NBTreeLeaf::NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr, LeafLoadMethod load)
    : NBTreeLeaf(read_block_from_bstore(bstore, curr), load)
{
}

NBTreeLeaf::NBTreeLeaf(std::shared_ptr<Block> block, LeafLoadMethod load)
    : prev_(EMPTY_ADDR)
{
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
    fanout_index_ = subtree->fanout_index;
}

size_t NBTreeLeaf::nelements() const {
    SubtreeRef const* subtree = subtree_cast(buffer_.data());
    return subtree->count;
}

u16 NBTreeLeaf::get_fanout() const {
    return fanout_index_;
}

aku_ParamId NBTreeLeaf::get_id() const {
    SubtreeRef const* subtree = subtree_cast(buffer_.data());
    return subtree->id;
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
    if (prev_ != EMPTY_ADDR) {
        subtree->addr = prev_;
    } else {
        // addr = EMPTY indicates that there is
        // no link to previous node.
        subtree->addr  = EMPTY_ADDR;
        // Invariant: fanout index should be 0 in this case.
        assert(fanout_index_ == 0);
    }
    subtree->version = AKUMULI_VERSION;
    subtree->level = 0;
    subtree->fanout_index = fanout_index_;
    return bstore->append_block(buffer_.data());
}


std::unique_ptr<NBTreeIterator> NBTreeLeaf::range(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<NBTreeIterator> it;
    it.reset(new NBTreeLeafIterator(begin, end, *this));
    return std::move(it);
}

std::unique_ptr<NBTreeIterator> NBTreeLeaf::search(aku_Timestamp begin, aku_Timestamp end, std::shared_ptr<BlockStore> bstore) const {
    // Traverse tree from largest timestamp to smallest
    aku_Timestamp min = std::min(begin, end);
    aku_Timestamp max = std::max(begin, end);
    LogicAddr addr = prev_;
    aku_Timestamp b, e;
    std::vector<std::unique_ptr<NBTreeIterator>> results;
    // Stop when EMPTY is hit or cycle detected.
    if (end <= begin) {
        // Backward direction - read data from this node at the beginning
        std::tie(b, e) = get_timestamps();
        if (!(e < min || max < b)) {
            results.push_back(std::move(range(begin, end)));
        }
    }
    while (bstore->exists(addr)) {
        std::unique_ptr<NBTreeLeaf> leaf;
        leaf.reset(new NBTreeLeaf(bstore, addr));
        std::tie(b, e) = leaf->get_timestamps();
        if (max < b) {
            break;
        }
        if (min > e) {
            addr = leaf->get_prev_addr();
            continue;
        }
        // Save address of the current leaf and move to the next one.
        results.push_back(std::move(leaf->range(begin, end)));
        addr = leaf->get_prev_addr();
    }
    if (begin < end) {
        // Forward direction - reverce results and read data from this node at the end
        std::reverse(results.begin(), results.end());
        std::tie(b, e) = get_timestamps();
        if (!(e < min || max < b)) {
            results.push_back(std::move(range(begin, end)));
        }
    }
    if (results.size() == 1) {
        return std::move(results.front());
    }
    std::unique_ptr<NBTreeIterator> res_iter;
    res_iter.reset(new IteratorConcat(std::move(results)));
    return std::move(res_iter);
}


// //////////////////////// //
//     NBTreeSuperblock     //
// //////////////////////// //

NBTreeSuperblock::NBTreeSuperblock(aku_ParamId id, LogicAddr prev, u16 fanout, u16 lvl)
    : buffer_(AKU_BLOCK_SIZE, 0)
    , id_(id)
    , write_pos_(0)
    , fanout_index_(fanout)
    , level_(lvl)
    , prev_(prev)
    , immutable_(false)
{
}

NBTreeSuperblock::NBTreeSuperblock(std::shared_ptr<Block> block)
    : buffer_(AKU_BLOCK_SIZE, 0)
    , immutable_(true)
{
    // Read content from bstore
    SubtreeRef const* ref = subtree_cast(block->get_data());
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    write_pos_ = ref->payload_size;
    level_ = ref->level;
    memcpy(buffer_.data(), block->get_data(), AKU_BLOCK_SIZE);
}

NBTreeSuperblock::NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore)
    : NBTreeSuperblock(read_block_from_bstore(bstore, addr))
{
}

NBTreeSuperblock::NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore, bool remove_last)
    : buffer_(AKU_BLOCK_SIZE, 0)
    , immutable_(false)
{
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(addr);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Bad arg, can't read data from block store, " + StatusUtil::str(status));
    }
    SubtreeRef const* ref = subtree_cast(block->get_data());
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    level_ = ref->level;
    write_pos_ = ref->payload_size;
    if (remove_last && write_pos_ != 0) {
        write_pos_--;
    }
    memcpy(buffer_.data(), block->get_data(), AKU_BLOCK_SIZE);
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
    return subtree_cast(buffer_.data())->addr;
}

aku_Status NBTreeSuperblock::append(const SubtreeRef &p) {
    if (is_full()) {
        return AKU_EOVERFLOW;
    }
    if (immutable_) {
        return AKU_EBAD_DATA;
    }
    // Write data into buffer
    SubtreeRef* pref = subtree_cast(buffer_.data());
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
    if (immutable_) {
        return std::make_tuple(AKU_EBAD_DATA, EMPTY_ADDR);
    }
    SubtreeRef* backref = subtree_cast(buffer_.data());
    if (fanout_index_ != 0) {
        NBTreeSuperblock subtree(prev_, bstore);
        aku_Status status = init_subtree_from_subtree(subtree, *backref);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, EMPTY_ADDR);
        }
        backref->addr = prev_;
    } else {
        backref->addr = EMPTY_ADDR;
    }
    // This fields should be rewrited to store node's own information
    backref->payload_size = static_cast<u16>(write_pos_);
    backref->fanout_index = fanout_index_;
    backref->id = id_;
    backref->level = level_;
    backref->version = AKUMULI_VERSION;
    return bstore->append_block(buffer_.data());
}

bool NBTreeSuperblock::is_full() const {
    return write_pos_ >= AKU_NBTREE_FANOUT;
}

aku_Status NBTreeSuperblock::read_all(std::vector<SubtreeRef>* refs) const {
    for(u32 ix = 0u; ix < write_pos_; ix++) {
        SubtreeRef const* ref = subtree_cast(buffer_.data());
        ref += (1 + ix);
        refs->push_back(*ref);
    }
    return AKU_SUCCESS;
}

std::tuple<aku_Timestamp, aku_Timestamp> NBTreeSuperblock::get_timestamps() const {
    SubtreeRef const* pref = subtree_cast(buffer_.data());
    return std::tie(pref->begin, pref->end);
}

//! Create subtree iterator
static std::unique_ptr<NBTreeIterator> get_subtree_iterator(SubtreeRef const& ref,
                                                            aku_Timestamp begin,
                                                            aku_Timestamp end,
                                                            std::shared_ptr<BlockStore> bstore)
{
    // Use BFS to iterate through the tree
    if (ref.level == 0) {
        // Points to leaf node
        NBTreeLeaf leaf(bstore, ref.addr);
        return std::move(leaf.range(begin, end));
    }
    NBTreeSuperblock sblock(ref.addr, bstore);
    return std::move(sblock.search(begin, end, bstore));
}

//! Return true if referenced subtree in [begin, end) range.
//! @note Begin should be less then end.
static bool subtree_in_range(SubtreeRef const& ref, aku_Timestamp begin, aku_Timestamp end) {
    if (ref.end < begin || end < ref.begin) {
        return false;
    }
    return true;
}


std::unique_ptr<NBTreeIterator> NBTreeSuperblock::search(aku_Timestamp begin,
                                                         aku_Timestamp end,
                                                         std::shared_ptr<BlockStore> bstore) const
{
    /* Algorithm outline:
     * - enumerate subtrees in right direction;
     * - call `range` recoursively
     * - concatenate iterators.
     */
    std::vector<SubtreeRef> refs;
    aku_Status status = read_all(&refs);
    if (status != AKU_SUCCESS) {
        // Create bad iterator that always returns error.
        std::unique_ptr<NBTreeIterator> p;
        p.reset(new NBTreeLeafIterator(status));
        return std::move(p);
    }
    auto min = std::min(begin, end);
    auto max = std::max(begin, end);
    std::vector<std::unique_ptr<NBTreeIterator>> iters;
    if (begin < end) {
        for (auto const& ref: refs) {
            if (subtree_in_range(ref, min, max)) {
                iters.push_back(std::move(get_subtree_iterator(ref, begin, end, bstore)));
            }
        }
    } else {
        for (auto it = refs.rbegin(); it < refs.rend(); it++) {
            if (subtree_in_range(*it, min, max)) {
                iters.push_back(std::move(get_subtree_iterator(*it, begin, end, bstore)));
            }
        }
    }
    if (iters.size() == 1) {
        return std::move(iters.front());
    }
    std::unique_ptr<NBTreeIterator> iter;
    iter.reset(new IteratorConcat(std::move(iters)));
    return std::move(iter);
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
    std::unique_ptr<NBTreeLeaf> leaf_;
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
            std::tie(status, block) = bstore_->read_block(last_);
            if (status != AKU_SUCCESS) {
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            }
            auto psubtree = subtree_cast(block->get_data());
            fanout_index_ = psubtree->fanout_index + 1;
            if (fanout_index_ == AKU_NBTREE_FANOUT) {
                fanout_index_ = 0;
                last_ = EMPTY_ADDR;
            }
        }
        reset_leaf();
    }

    aku_Status get_prev_subtreeref(SubtreeRef &payload) {
        aku_Status status = AKU_SUCCESS;
        NBTreeLeaf leaf(bstore_, last_, NBTreeLeaf::LeafLoadMethod::FULL_PAGE_LOAD);
        status = init_subtree_from_leaf(leaf, payload);
        payload.addr = last_;
        return status;
    }

    u16 get_current_fanout_index() const {
        return leaf_->get_fanout();
    }

    void reset_leaf() {
        leaf_.reset(new NBTreeLeaf(id_, last_, fanout_index_));
    }

    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value);
    virtual std::tuple<bool, LogicAddr> append(const SubtreeRef &pl);
    virtual std::tuple<bool, LogicAddr> commit(bool final);
    virtual std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end) const;
    virtual bool is_dirty() const;
};

std::tuple<bool, LogicAddr> NBTreeLeafExtent::append(SubtreeRef const&) {
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
        AKU_PANIC("Can't write leaf-node to block-store, " + StatusUtil::str(status));
    }
    // Gather stats and send them to upper-level node
    SubtreeRef payload;
    status = init_subtree_from_leaf(*leaf_, payload);
    if (status != AKU_SUCCESS) {
        // This shouldn't happen because leaf node can't be
        // empty just after overflow.
        AKU_PANIC("Can summarize leaf-node - " + StatusUtil::str(status));
    }
    payload.addr = addr;
    bool parent_saved = false;
    auto roots_collection = roots_.lock();
    size_t next_level = payload.level + 1;
    if (roots_collection) {
        if (!final || roots_collection->get_roots().size() > next_level) {
            parent_saved = roots_collection->append(payload);
        }
    } else {
        // Invariant broken.
        // Roots collection was destroyed before write process
        // stops.
        AKU_PANIC("Roots collection destroyed");
    }
    fanout_index_++;
    if (fanout_index_ == AKU_NBTREE_FANOUT) {
        fanout_index_ = 0;
        last_ = EMPTY_ADDR;
    }
    last_ = addr;
    reset_leaf();
    // NOTE: we should reset current extent's rescue point because parent node was saved and
    // already has a link to current extent (e.g. leaf node was saved and new leaf
    // address was added to level 1 node, level 1 node becomes full and was written to disk).
    // If we won't do this - we will read the same information twice during crash recovery process.
    return std::make_tuple(parent_saved, addr);
}

std::unique_ptr<NBTreeIterator> NBTreeLeafExtent::search(aku_Timestamp begin, aku_Timestamp end) const {
    return std::move(leaf_->range(begin, end));
}

bool NBTreeLeafExtent::is_dirty() const {
    if (leaf_) {
        return leaf_->nelements() != 0;
    }
    return false;
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
    u32 pad_;

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
        , pad_{}
    {
        if (addr != EMPTY_ADDR) {
            // `addr` is not empty. Node should be restored from
            // block-store.
            aku_Status status;
            std::shared_ptr<Block> block;
            std::tie(status, block) = bstore_->read_block(addr);
            if (status != AKU_SUCCESS) {
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            }
            auto psubtree = subtree_cast(block->get_data());
            fanout_index_ = psubtree->fanout_index + 1;
            if (fanout_index_ == AKU_NBTREE_FANOUT) {
                fanout_index_ = 0;
                last_ = EMPTY_ADDR;
            }
            last_ = psubtree->addr;
            // CoW constructor should be used here.
            curr_.reset(new NBTreeSuperblock(addr, bstore_, false));
        } else {
            // `addr` is not set. Node should be created from scratch.
            curr_.reset(new NBTreeSuperblock(id, EMPTY_ADDR, 0, level));
        }
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

    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value);
    virtual std::tuple<bool, LogicAddr> append(const SubtreeRef &pl);
    virtual std::tuple<bool, LogicAddr> commit(bool final);
    virtual std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end) const;
    virtual bool is_dirty() const;
};

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
        return std::make_tuple(true, addr);
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
        AKU_PANIC("Can't write leaf-node to block-store, " + StatusUtil::str(status));
    }
    // Gather stats and send them to upper-level node
    SubtreeRef payload;
    status = init_subtree_from_subtree(*curr_, payload);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can summarize current node - " + StatusUtil::str(status));
    }
    payload.addr = addr;
    bool parent_saved = false;
    auto roots_collection = roots_.lock();
    size_t next_level = payload.level + 1;
    if (roots_collection) {
        if (!final || roots_collection->get_roots().size() > next_level) {
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
    if (fanout_index_ == AKU_NBTREE_FANOUT) {
        fanout_index_ = 0;
        last_ = EMPTY_ADDR;
    }
    last_ = addr;
    reset_subtree();
    // NOTE: we should reset current extent's rescue point because parent node was saved and
    // parent node already has a link to this extent.
    return std::make_tuple(parent_saved, addr);
}

std::unique_ptr<NBTreeIterator> NBTreeSBlockExtent::search(aku_Timestamp begin, aku_Timestamp end) const {
    return std::move(curr_->search(begin, end, bstore_));
}

bool NBTreeSBlockExtent::is_dirty() const {
    if (curr_) {
        return curr_->nelements() != 0;
    }
    return false;
}


static void check_superblock_consistency(std::shared_ptr<BlockStore> bstore, NBTreeSuperblock const* sblock, u16 required_level) {
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
        // Try to read block and check stats
        std::shared_ptr<Block> block;
        std::tie(status, block) = bstore->read_block(refs[i].addr);
        if (status == AKU_ENO_DATA) {
            // block was deleted due to retention.
            Logger::msg(AKU_LOG_INFO, "Block " + std::to_string(refs[i].addr));
        } else if (status == AKU_SUCCESS) {
            SubtreeRef out;
            if (required_level == 0) {
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
    if (required_level > 0) {
        for (auto addr: nodes2follow) {
            NBTreeSuperblock child(addr, bstore);
            check_superblock_consistency(bstore, &child, required_level - 1);
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
        check_superblock_consistency(bstore, curr, static_cast<u16>(level - 1));
    }
}

// ///////////////////// //
//   NBTreeExtentsList   //
// ///////////////////// //


NBTreeExtentsList::NBTreeExtentsList(aku_ParamId id, std::vector<LogicAddr> addresses, std::shared_ptr<BlockStore> bstore)
    : bstore_(bstore)
    , id_(id)
    , rescue_points_(std::move(addresses))
    , initialized_(false)
{
    if (rescue_points_.size() >= std::numeric_limits<u16>::max()) {
        AKU_PANIC("Tree depth is too large");
    }
}

void NBTreeExtentsList::force_init() {
    if (!initialized_) {
        init();
    }
}

std::vector<NBTreeExtent const*> NBTreeExtentsList::get_extents() const {
    std::vector<NBTreeExtent const*> result;
    for (auto const& ptr: extents_) {
        result.push_back(ptr.get());
    }
    return result;
}

bool NBTreeExtentsList::append(aku_Timestamp ts, double value) {
    if (!initialized_) {
        init();
    }
    if (extents_.size() == 0) {
        // create first leaf node
        std::unique_ptr<NBTreeExtent> leaf;
        leaf.reset(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, EMPTY_ADDR));
        extents_.push_back(std::move(leaf));
        rescue_points_.push_back(EMPTY_ADDR);
    }
    bool parent_saved = false;
    LogicAddr addr = EMPTY_ADDR;
    std::tie(parent_saved, addr) = extents_.front()->append(ts, value);
    if (addr != EMPTY_ADDR) {
        if (parent_saved) {
            // NOTE: node at `addr` was saved into parent node and
            // parent node was saved to disk. No need to store this
            // address as leaf's rescue point!
            addr = EMPTY_ADDR;
        }
        if (rescue_points_.size() > 0) {
            rescue_points_.at(0) = addr;
        } else {
            rescue_points_.push_back(addr);
        }
        return true;
    }
    return false;
}

bool NBTreeExtentsList::append(const SubtreeRef &pl) {
    if (!initialized_) {
        init();
    }
    u16 lvl = static_cast<u16>(pl.level + 1);
    NBTreeExtent* root = nullptr;
    if (extents_.size() > lvl) {
        // Fast path
        root = extents_[lvl].get();
    } else if (extents_.size() == lvl) {
        std::unique_ptr<NBTreeExtent> p;
        p.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(),
                                       id_, EMPTY_ADDR, lvl));
        root = p.get();
        extents_.push_back(std::move(p));
        rescue_points_.push_back(EMPTY_ADDR);
    } else {
        Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Invalid node level - " + std::to_string(lvl));
        AKU_PANIC("Invalid node level");
    }
    bool parent_saved = false;
    LogicAddr addr = EMPTY_ADDR;
    std::tie(parent_saved, addr) = root->append(pl);
    if (addr != EMPTY_ADDR) {
        // NOTE: `addr != EMPTY_ADDR` means that something was saved to disk (current node or parent node).
        if (parent_saved) {
            // NOTE: node at `addr` was saved into parent node and
            // parent node was saved to disk. No need to store this
            // address in internal node's rescue point!
            addr = EMPTY_ADDR;
        }
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

void NBTreeExtentsList::init() {
    initialized_ = true;
    if (rescue_points_.empty() == false) {
        auto rstat = repair_status(rescue_points_);
        // Tree should be opened normally.
        if (rstat == RepairStatus::OK) {
            Logger::msg(AKU_LOG_INFO, std::to_string(id_) + " Trying to open tree, repair status - OK, addr: " + std::to_string(rescue_points_.back()));
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
                std::tie(status, leaf_block) = bstore_->read_block(addr);
                if (status != AKU_SUCCESS) {
                    // Tree is old and should be removed, no data was left on the block device.
                    // FIXME: handle obsolete trees correctly!
                    Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Obsolete tree handling not implemented");
                    initialized_ = false;
                    return;
                }
                NBTreeLeaf leaf(leaf_block);  // fully loaded leaf
                SubtreeRef sref = {};
                status = init_subtree_from_leaf(leaf, sref);
                if (status != AKU_SUCCESS) {
                    Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't open tree at: " + std::to_string(addr) +
                                " error: " + StatusUtil::str(status));
                    AKU_PANIC("Can't open tree");
                }
                root_extent->append(sref);  // this always should return `false` and `EMPTY_ADDR`, no need to check this.

                // Create new empty leaf
                std::unique_ptr<NBTreeExtent> leaf_extent(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, addr));
                extents_.push_back(std::move(leaf_extent));
                extents_.push_back(std::move(root_extent));
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
        }
        // Tree should be restored (crush recovery kicks in here).
        else {
            AKU_PANIC("Not implemented");
            // Construct roots using CoW
            int i = static_cast<int>(rescue_points_.size());
            while (i --> 0) {
                if (i == 0) {
                    // create leaf
                    std::unique_ptr<NBTreeLeafExtent> leaf;
                    auto addr = rescue_points_[i];
                    leaf.reset(new NBTreeLeafExtent(bstore_, shared_from_this(), id_, addr));
                    NBTreeLeafExtent* proot = leaf.get();
                    extents_.push_front(std::move(leaf));
                    aku_Status status;
                    SubtreeRef payload;
                    status = proot->get_prev_subtreeref(payload);
                    if (status == AKU_SUCCESS) {
                        append(payload);
                    } else {
                        AKU_PANIC("Can't restore tree at " + std::to_string(addr) + " error: " + StatusUtil::str(status));
                    }
                } else {
                    // create superblock
                    std::unique_ptr<NBTreeSBlockExtent> p;
                    u16 lvl = static_cast<u16>(i);
                    auto addr = rescue_points_[i];
                    p.reset(new NBTreeSBlockExtent(bstore_, shared_from_this(), id_, addr, lvl));
                    // Reconstruct upper level node if needed!
                    LogicAddr prev = p->get_prev_addr();
                    if (extents_.empty() && prev != EMPTY_ADDR) {
                        // If there is previous node and there is no higher level node then
                        // higher level node was stored in memory and was lost.
                        std::vector<LogicAddr> addrlist;
                        std::vector<SubtreeRef> reflist;
                        // We should read up to AKU_NBTREE_FANOUT items!
                        size_t ix = 0;
                        while (prev != EMPTY_ADDR) {
                            if (ix > AKU_NBTREE_FANOUT) {
                                AKU_PANIC("Ill-formed tree, too many nodes.");
                            }
                            NBTreeSuperblock sblock(prev, bstore_);
                            addrlist.push_back(prev);
                            SubtreeRef ref;
                            auto status = init_subtree_from_subtree(sblock, ref);
                            if (status != AKU_SUCCESS) {
                                // TODO: we should remove roots collection from the data-set in this case
                                AKU_PANIC("Can't restore tree");
                            }
                            ref.addr = prev;
                            reflist.push_back(ref);
                            prev = sblock.get_prev_addr();
                            ix++;
                        }
                        // Reconstructed node
                        //roots_.push_front(std::move(...));
                    }
                    extents_.push_front(std::move(p));
                }
            }
        }
    }
}

std::unique_ptr<NBTreeIterator> NBTreeExtentsList::search(aku_Timestamp begin, aku_Timestamp end) const {
    if (!initialized_) {
        // FIXME: so ugly!
        const_cast<NBTreeExtentsList*>(this)->init();
    }
    std::vector<std::unique_ptr<NBTreeIterator>> iterators;
    if (begin < end) {
        for (auto it = extents_.rbegin(); it != extents_.rend(); it++) {
            iterators.push_back(std::move((*it)->search(begin, end)));
        }
    } else {
        for (auto const& root: extents_) {
            iterators.push_back(std::move(root->search(begin, end)));
        }
    }
    if (iterators.size() == 1) {
        return std::move(iterators.front());
    }
    std::unique_ptr<NBTreeIterator> concat;
    concat.reset(new IteratorConcat(std::move(iterators)));
    return std::move(concat);
}


std::vector<LogicAddr> NBTreeExtentsList::close() {
    if (initialized_) {
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
    return rescue_points_;
}

NBTreeExtentsList::RepairStatus NBTreeExtentsList::repair_status(std::vector<LogicAddr> rescue_points) {
    ssize_t count = static_cast<ssize_t>(rescue_points.size()) -
                    std::count(rescue_points.begin(), rescue_points.end(), EMPTY_ADDR);
    if (count == 1 && rescue_points.back() != EMPTY_ADDR) {
        return RepairStatus::OK;
    }
    return RepairStatus::REPAIR;
}


static NBTreeBlockType _dbg_get_block_type(std::shared_ptr<Block> block) {
    auto ref = reinterpret_cast<SubtreeRef const*>(block->get_data());
    return ref->level == 0 ? NBTreeBlockType::LEAF : NBTreeBlockType::INNER;
}

void NBTreeExtentsList::debug_print(LogicAddr root, std::shared_ptr<BlockStore> bstore, size_t depth) {
    std::string pad(depth, ' ');
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(root);
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
