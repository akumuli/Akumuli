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

// App
#include "nbtree.h"
#include "akumuli_version.h"
#include "status_util.h"
#include "log_iface.h"


namespace Akumuli {
namespace StorageEngine {

static const SubtreeRef INIT_SUBTREE_REF = {
    0,
    //! Series Id
    0,
    //! First element's timestamp
    0,
    //! Last element's timestamp
    0,
    //! Object addr in blockstore
    EMPTY_ADDR,
    //! Smalles value
    std::numeric_limits<double>::max(),
    //! Registration time of the smallest value
    std::numeric_limits<aku_Timestamp>::max(),
    //! Largest value
    std::numeric_limits<double>::min(),
    //! Registration time of the largest value
    std::numeric_limits<aku_Timestamp>::min(),
    //! Summ of all elements in subtree
    .0,
    //! First value in subtree
    .0,
    //! Last value in subtree
    .0,
    //! Node version
    AKUMULI_VERSION,
    //! Node level in the tree
    0,
    //! Payload size (real)
    0,
    //! Fan out index of the element (current)
    0,
    //! Checksum of the block (not used for links to child nodes)
    0
};

void NBTreeAggregationResult::copy_from(SubtreeRef const& r) {
    cnt = r.count;
    sum = r.sum;
    min = r.min;
    max = r.max;
    mints = r.min_time;
    maxts = r.max_time;
    first = r.first;
    last = r.last;
    _begin = r.begin;
    _end = r.end;
}

void NBTreeAggregationResult::do_the_math(aku_Timestamp* tss, double const* xss, size_t size, bool inverted) {
    assert(size);
    cnt += size;
    for (size_t i = 0; i < size; i++) {
        sum += xss[i];
        if (min > xss[i]) {
            min = xss[i];
            mints = tss[i];
        }
        if (max < xss[i]) {
            max = xss[i];
            maxts = tss[i];
        }
        min = std::min(min, xss[i]);
        max = std::max(max, xss[i]);
    }
    if (!inverted) {
        first = xss[0];
        last = xss[size - 1];
        _begin = tss[0];
        _end = tss[size - 1];
    } else {
        last = xss[0];
        first = xss[size - 1];
        _end = tss[0];
        _begin = tss[size - 1];
    }
}

void NBTreeAggregationResult::combine(const NBTreeAggregationResult& other) {
    sum += other.sum;
    cnt += other.cnt;
    if (min > other.min) {
        min = other.min;
        mints = other.mints;
    }
    if (max < other.max) {
        max = other.max;
        maxts = other.maxts;
    }
    min = std::min(min, other.min);
    max = std::max(max, other.max);
    if (_begin > other._begin) {
        first = other.first;
        _begin = other._begin;
    }
    if (_end < other._end) {
        last = other.last;
        _end = other._end;
    }
}

static SubtreeRef* subtree_cast(u8* p) {
    return reinterpret_cast<SubtreeRef*>(p);
}

static SubtreeRef const* subtree_cast(u8 const* p) {
    return reinterpret_cast<SubtreeRef const*>(p);
}


static std::tuple<aku_Status, std::shared_ptr<Block>> read_and_check(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        return std::tie(status, block);
    }
    // Check consistency (works with both inner and leaf nodes).
    u8 const* data = block->get_data();
    SubtreeRef const* subtree = subtree_cast(data);
    u32 crc = bstore->checksum(data + sizeof(SubtreeRef), subtree->payload_size);
    if (crc != subtree->checksum) {
        std::stringstream fmt;
        fmt << "Invalid checksum (addr: " << curr << ", level: " << subtree->level << ")";
        Logger::msg(AKU_LOG_ERROR, fmt.str());
        status = AKU_EBAD_DATA;
    }
    return std::tie(status, block);
}


//! Read block from blockstoroe with all the checks. Panic on error!
static std::shared_ptr<Block> read_block_from_bstore(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Can't read block - " + StatusUtil::str(status));
    }
    // Check consistency (works with both inner and leaf nodes).
    u8 const* data = block->get_data();
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
static aku_Status init_subtree_from_leaf(const NBTreeLeaf& leaf, SubtreeRef& out) {
    if (leaf.nelements() == 0) {
        return AKU_EBAD_ARG;
    }
    SubtreeRef const* meta = leaf.get_leafmeta();
    out = *meta;
    out.payload_size = 0;
    out.checksum = 0;
    out.addr = EMPTY_ADDR;  // Leaf metadta stores address of the previous node!
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
    //! Padding
    u32 pad_;

    NBTreeLeafIterator(aku_Status status)
        : begin_()
        , end_()
        , from_()
        , to_()
        , status_(status)
    {
    }

    NBTreeLeafIterator(aku_Timestamp begin, aku_Timestamp end, NBTreeLeaf const& node, bool delay_init=false)
        : begin_(begin)
        , end_(end)
        , from_()
        , to_()
        , status_(AKU_ENO_DATA)
    {
        if (!delay_init) {
            init(node);
        }
    }

    void init(NBTreeLeaf const& node) {
        aku_Timestamp min = std::min(begin_, end_);
        aku_Timestamp max = std::max(begin_, end_);
        aku_Timestamp nb, ne;
        std::tie(nb, ne) = node.get_timestamps();
        if (max < nb || ne < min) {
            status_ = AKU_ENO_DATA;
            return;
        }
        status_ = node.read_all(&tsbuf_, &xsbuf_);
        if (status_ == AKU_SUCCESS) {
            if (begin_ < end_) {
                // FWD direction
                auto it_begin = std::lower_bound(tsbuf_.begin(), tsbuf_.end(), begin_);
                if (it_begin != tsbuf_.end()) {
                    from_ = std::distance(tsbuf_.begin(), it_begin);
                } else {
                    from_ = 0;
                    assert(tsbuf_.front() > begin_);
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

    size_t get_size() const {
        assert(to_ >= from_);
        return static_cast<size_t>(to_ - from_);
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size);
    virtual Direction get_direction();
};


std::tuple<aku_Status, size_t> NBTreeLeafIterator::read(aku_Timestamp *destts, double *destval, size_t size) {
    ssize_t sz = static_cast<ssize_t>(size);
    if (status_ != AKU_SUCCESS) {
        return std::make_tuple(status_, 0);
    }
    ssize_t toread = to_ - from_;
    if (toread > sz) {
        toread = sz;
    }
    if (toread == 0) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }
    auto begin = from_;
    ssize_t end = from_ + toread;
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
    aku_Status status = AKU_ENO_DATA;
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
        if (status == AKU_ENO_DATA || status == AKU_EUNAVAILABLE) {
            // this leaf node is empty or removed, continue with next
            iter_index_++;
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

// //////////////////////////// //
//  NBTreeIterator aggregation  //
// //////////////////////////// //

/** Aggregating iterator.
  * Accepts list of iterators in the c-tor. All iterators then
  * can be seen as one iterator that returns single value.
  */
struct IteratorAggregate : NBTreeAggregator {
    typedef std::vector<std::unique_ptr<NBTreeAggregator>> IterVec;
    IterVec             iter_;
    Direction           dir_;
    u32                 iter_index_;

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    IteratorAggregate(TVec&& iter)
        : iter_(std::forward<TVec>(iter))
        , iter_index_(0)
    {
        if (iter_.empty()) {
            dir_ = Direction::FORWARD;
        } else {
            dir_ = iter_.front()->get_direction();
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size);
    virtual Direction get_direction();
};

std::tuple<aku_Status, size_t> IteratorAggregate::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
    if (size == 0) {
        return std::make_tuple(AKU_EBAD_ARG, 0);
    }
    if (iter_index_ == iter_.size()) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }
    const size_t SZBUF = 1024;
    aku_Status status = AKU_ENO_DATA;
    NBTreeAggregationResult xsresult = INIT_AGGRES;
    aku_Timestamp tsresult = 0;
    std::vector<NBTreeAggregationResult> outval(SZBUF, INIT_AGGRES);
    std::vector<aku_Timestamp> outts(SZBUF, 0);
    ssize_t ressz;
    while(iter_index_ < iter_.size()) {
        std::tie(status, ressz) = iter_[iter_index_]->read(outts.data(), outval.data(), SZBUF);
        if (ressz != 0) {
            xsresult = std::accumulate(outval.begin(), outval.begin() + ressz, xsresult,
                            [](NBTreeAggregationResult lhs, NBTreeAggregationResult rhs) {
                                lhs.combine(rhs);
                                return lhs;
                            });
            tsresult = outts[static_cast<size_t>(ressz)-1];
        }
        if (status == AKU_ENO_DATA) {
            // This leaf node is empty, continue with next.
            iter_index_++;
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Failure, stop iteration.
            return std::make_pair(status, 0);
        }
    }
    destts[0] = tsresult;
    destval[0] = xsresult;
    return std::make_tuple(AKU_SUCCESS, 1);
}

NBTreeAggregator::Direction IteratorAggregate::get_direction() {
    return dir_;
}

// ///////////////////////// //
//    Superblock Iterator    //
// ///////////////////////// //

//! Return true if referenced subtree in [begin, end) range.
//! @note Begin should be less then end.
static bool subtree_in_range(SubtreeRef const& ref, aku_Timestamp begin, aku_Timestamp end) {
    if (ref.end < begin || end < ref.begin) {
        return false;
    }
    return true;
}

template<class TVal>
struct NBTreeSBlockIteratorBase : NBTreeIteratorBase<TVal> {
    //! Starting timestamp
    aku_Timestamp              begin_;
    //! Final timestamp
    aku_Timestamp              end_;
    //! Address of the current superblock
    LogicAddr addr_;
    //! Blockstore
    std::shared_ptr<BlockStore> bstore_;

    // FSM
    std::vector<SubtreeRef> refs_;
    std::unique_ptr<NBTreeIteratorBase<TVal>> iter_;
    u32 fsm_pos_;
    i32 refs_pos_;

    typedef std::unique_ptr<NBTreeIteratorBase<TVal>> TIter;
    typedef typename NBTreeIteratorBase<TVal>::Direction Direction;

    NBTreeSBlockIteratorBase(std::shared_ptr<BlockStore> bstore, LogicAddr addr, aku_Timestamp begin, aku_Timestamp end)
        : begin_(begin)
        , end_(end)
        , addr_(addr)
        , bstore_(bstore)
        , fsm_pos_(0)
        , refs_pos_(0)
    {
    }

    NBTreeSBlockIteratorBase(std::shared_ptr<BlockStore> bstore, NBTreeSuperblock const& sblock, aku_Timestamp begin, aku_Timestamp end)
        : begin_(begin)
        , end_(end)
        , addr_(EMPTY_ADDR)
        , bstore_(bstore)
        , fsm_pos_(1)  // FSM will bypass `init` step.
        , refs_pos_(0)
    {
        aku_Status status = sblock.read_all(&refs_);
        if (status != AKU_SUCCESS) {
            // `read` call should fail with AKU_ENO_DATA error.
            refs_pos_ = begin_ < end_ ? static_cast<i32>(refs_.size()) : -1;
        } else {
            refs_pos_ = begin_ < end_ ? 0 : static_cast<i32>(refs_.size()) - 1;
        }
    }

    aku_Status init() {
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, addr_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        NBTreeSuperblock current(block);
        status = current.read_all(&refs_);
        refs_pos_ = begin_ < end_ ? 0 : static_cast<i32>(refs_.size()) - 1;
        return status;
    }

    //! Create leaf iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_leaf_iterator(const SubtreeRef &ref) = 0;

    //! Create superblock iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_superblock_iterator(const SubtreeRef &ref) = 0;

    //! This is a template method, aggregator should derive from this object and
    //! override make_*_iterator virtual methods to customize iterator's behavior.
    std::tuple<aku_Status, TIter> get_next_iter() {
        auto min = std::min(begin_, end_);
        auto max = std::max(begin_, end_);

        TIter empty;
        SubtreeRef ref = INIT_SUBTREE_REF;
        if (get_direction() == Direction::FORWARD) {
            if (refs_pos_ == static_cast<i32>(refs_.size())) {
                // Done
                return std::make_tuple(AKU_ENO_DATA, std::move(empty));
            }
            ref = refs_.at(static_cast<size_t>(refs_pos_));
            refs_pos_++;
        } else {
            if (refs_pos_ < 0) {
                // Done
                return std::make_tuple(AKU_ENO_DATA, std::move(empty));
            }
            ref = refs_.at(static_cast<size_t>(refs_pos_));
            refs_pos_--;
        }
        std::tuple<aku_Status, TIter> result;
        if (!subtree_in_range(ref, min, max)) {
            // Subtree not in [begin_, end_) range. Proceed to next.
            result = std::make_tuple(AKU_ENOT_FOUND, std::move(empty));
        } else if (ref.level == 0) {
            result = std::move(make_leaf_iterator(ref));
        } else {
            result = std::move(make_superblock_iterator(ref));
        }
        return std::move(result);
    }

    //! Iteration implementation. Can be customized in derived classes.
    std::tuple<aku_Status, size_t> iter(aku_Timestamp *destts, TVal *destval, size_t size) {
        // Main loop, draw data from iterator till out array become empty.
        size_t out_size = 0;
        aku_Status status = AKU_ENO_DATA;
        while(out_size < size) {
            if (!iter_) {
                // initialize `iter_`
                std::tie(status, iter_) = get_next_iter();
                if (status == AKU_ENOT_FOUND || status == AKU_EUNAVAILABLE) {
                    // Subtree exists but doesn't contains values from begin-end timerange or
                    // entire subtree was deleted
                    // TODO: remove
                    Logger::msg(AKU_LOG_TRACE, "Can't open next iterator because " + StatusUtil::str(status));
                    continue;
                } else if (status != AKU_SUCCESS) {
                    // We're out of iterators and should stop.
                    break;
                }
            }
            size_t sz;
            std::tie(status, sz) = iter_->read(destts + out_size, destval + out_size, size - out_size);
            out_size += sz;
            if (status == AKU_ENO_DATA) {
                // Move to next iterator.
                iter_.reset();
            } else if (status != AKU_SUCCESS) {
                // Unexpected error, can't proceed.
                break;
            }
        }
        return std::make_tuple(status, out_size);
    }

    virtual Direction get_direction() {
        if (begin_ < end_) {
            return Direction::FORWARD;
        }
        return Direction::BACKWARD;
    }
};


struct NBTreeSBlockIterator : NBTreeSBlockIteratorBase<double> {

    NBTreeSBlockIterator(std::shared_ptr<BlockStore> bstore, LogicAddr addr, aku_Timestamp begin, aku_Timestamp end)
        : NBTreeSBlockIteratorBase<double>(bstore, addr, begin, end)
    {
    }

    NBTreeSBlockIterator(std::shared_ptr<BlockStore> bstore, NBTreeSuperblock const& sblock, aku_Timestamp begin, aku_Timestamp end)
        : NBTreeSBlockIteratorBase<double>(bstore, sblock, begin, end)
    {
    }

    //! Create leaf iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_leaf_iterator(const SubtreeRef &ref) {
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, ref.addr);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, std::unique_ptr<NBTreeIterator>());
        }
        NBTreeLeaf leaf(block);
        std::unique_ptr<NBTreeIterator> result;
        result.reset(new NBTreeLeafIterator(begin_, end_, leaf));
        return std::make_tuple(AKU_SUCCESS, std::move(result));
    }

    //! Create superblock iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_superblock_iterator(const SubtreeRef &ref) {
        TIter result;
        result.reset(new NBTreeSBlockIterator(bstore_, ref.addr, begin_, end_));
        return std::make_tuple(AKU_SUCCESS, std::move(result));
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size) {
        if (!fsm_pos_ ) {
            aku_Status status = AKU_SUCCESS;
            status = init();
            if (status != AKU_SUCCESS) {
                return std::make_pair(status, 0ul);
            }
            fsm_pos_++;
        }
        return iter(destts, destval, size);
    }
};

// //////////////////// //
// NBTreeLeafAggregator //
// //////////////////// //

class NBTreeLeafAggregator : public NBTreeAggregator {
    NBTreeLeafIterator iter_;
    bool enable_cached_metadata_;
    SubtreeRef metacache_;
public:
    NBTreeLeafAggregator(aku_Timestamp begin, aku_Timestamp end, NBTreeLeaf const& node)
        : iter_(begin, end, node, true)
        , enable_cached_metadata_(false)
        , metacache_(INIT_SUBTREE_REF)
    {
        aku_Timestamp nodemin, nodemax, min, max;
        std::tie(nodemin, nodemax) = node.get_timestamps();
        min = std::min(begin, end);
        max = std::max(begin, end);
        if (min <= nodemin && nodemax < max) {
            // Leaf totally inside the search range, we can use metadata.
            metacache_ = *node.get_leafmeta();
            enable_cached_metadata_ = true;
        } else {
            // Otherwise we need to compute aggregate from subset of leaf's values.
            iter_.init(node);
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, NBTreeAggregationResult *destxs, size_t size);
    virtual Direction get_direction();
};

NBTreeLeafAggregator::Direction NBTreeLeafAggregator::get_direction() {
    return iter_.get_direction() == NBTreeLeafIterator::Direction::FORWARD ? Direction::FORWARD : Direction::BACKWARD;
}

std::tuple<aku_Status, size_t> NBTreeLeafAggregator::read(aku_Timestamp *destts, NBTreeAggregationResult *destxs, size_t size) {
    aku_Timestamp outts = 0;
    NBTreeAggregationResult outval = INIT_AGGRES;
    if (size == 0) {
        return std::make_tuple(AKU_EBAD_ARG, 0);
    }
    if (enable_cached_metadata_) {
        // Fast path. Use metadata to compute results.
        outval.copy_from(metacache_);
        outts = metacache_.begin;
        enable_cached_metadata_ = false;
        // next call to `read` should return AKU_ENO_DATA
    } else {
        if (!iter_.get_size()) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        size_t size_hint = iter_.get_size();
        std::vector<double> xs(size_hint, .0);
        std::vector<aku_Timestamp> ts(size_hint, 0);
        aku_Status status;
        size_t out_size;
        std::tie(status, out_size) = iter_.read(ts.data(), xs.data(), size_hint);
        if (status != AKU_SUCCESS) {
            return std::tie(status, out_size);
        }
        if (out_size == 0) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        assert(out_size == size_hint);
        bool inverted = iter_.get_direction() == NBTreeLeafIterator::Direction::BACKWARD;
        outval.do_the_math(ts.data(), xs.data(), out_size, inverted);
        outts = ts.front();  // INVARIANT: ts.size() is gt 0, destts(xs) size is gt 0
    }
    destts[0] = outts;
    destxs[0] = outval;
    return std::make_tuple(AKU_SUCCESS, 1);
}


// ////////////////////// //
// NBTreeSBlockAggregator //
// ////////////////////// //

/** Aggregator that returns precomputed value.
  * Value should be set in c-tor.
  */
class ValueAggregator : public NBTreeAggregator {
    aku_Timestamp ts_;
    NBTreeAggregationResult value_;
    Direction dir_;
    bool used_;
public:
    ValueAggregator(aku_Timestamp ts, NBTreeAggregationResult value, Direction dir)
        : ts_(ts)
        , value_(value)
        , dir_(dir)
        , used_(false)
    {
    }

    ValueAggregator()
        : ts_()
        , value_()
        , dir_()
        , used_(true)
    {}

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) override;
    virtual Direction get_direction() override;
};

std::tuple<aku_Status, size_t> ValueAggregator::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
    if (size == 0) {
        return std::make_pair(AKU_EBAD_ARG, 0);
    }
    if (used_) {
        return std::make_pair(AKU_ENO_DATA, 0);
    }
    used_ = true;
    destval[0] = value_;
    destts[0] = ts_;
    return std::make_pair(AKU_SUCCESS, 1);
}

ValueAggregator::Direction ValueAggregator::get_direction() {
    return dir_;
}

/** Superblock aggregator (iterator that computes different aggregates e.g. min/max/avg/sum).
  * Uses metadata stored in superblocks in some cases.
  */
class NBTreeSBlockAggregator : public NBTreeSBlockIteratorBase<NBTreeAggregationResult> {

public:
    NBTreeSBlockAggregator(std::shared_ptr<BlockStore> bstore,
                           NBTreeSuperblock const& sblock,
                           aku_Timestamp begin,
                           aku_Timestamp end)
        : NBTreeSBlockIteratorBase<NBTreeAggregationResult>(bstore, sblock, begin, end)
    {
    }

    NBTreeSBlockAggregator(std::shared_ptr<BlockStore> bstore,
                           LogicAddr addr,
                           aku_Timestamp begin,
                           aku_Timestamp end)
        : NBTreeSBlockIteratorBase<NBTreeAggregationResult>(bstore, addr, begin, end)
    {
    }
    virtual std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> make_leaf_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> make_superblock_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) override;
};

std::tuple<aku_Status, size_t> NBTreeSBlockAggregator::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
    if (size == 0) {
        return std::make_pair(AKU_EBAD_ARG, 0ul);
    }
    if (!fsm_pos_ ) {
        aku_Status status = AKU_SUCCESS;
        status = init();
        if (status != AKU_SUCCESS) {
            return std::make_pair(status, 0ul);
        }
        fsm_pos_++;
    }
    size_t SZBUF = 1024;
    std::vector<NBTreeAggregationResult> xss(SZBUF, INIT_AGGRES);
    std::vector<aku_Timestamp> tss(SZBUF, 0);
    aku_Timestamp outts = 0;
    NBTreeAggregationResult outxs = INIT_AGGRES;
    ssize_t outsz = 0;
    aku_Status status;
    while(true) {
        std::tie(status, outsz) = iter(tss.data(), xss.data(), SZBUF);
        if ((status == AKU_SUCCESS || status == AKU_ENO_DATA) && outsz != 0) {
            outts = tss[static_cast<size_t>(outsz)];
            outxs = std::accumulate(xss.begin(), xss.begin() + outsz, outxs,
                        [&](NBTreeAggregationResult lhs, NBTreeAggregationResult rhs) {
                            lhs.combine(rhs);
                            return lhs;
                        });
            size = 1;
        } else if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
            size = 0;
            break;
        } else if (outsz == 0) {
            destval[0] = outxs;
            destts[0] = outts;
            break;
        }
    }
    return std::make_tuple(status, size);
}

std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator> > NBTreeSBlockAggregator::make_leaf_iterator(SubtreeRef const& ref) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = read_and_check(bstore_, ref.addr);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<NBTreeAggregator>());
    }
    NBTreeLeaf leaf(block);
    std::unique_ptr<NBTreeAggregator> result;
    result.reset(new NBTreeLeafAggregator(begin_, end_, leaf));
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator> > NBTreeSBlockAggregator::make_superblock_iterator(SubtreeRef const& ref) {
    aku_Timestamp min = std::min(begin_, end_);
    aku_Timestamp max = std::max(begin_, end_);
    std::unique_ptr<NBTreeAggregator> result;
    if (min <= ref.begin && ref.end < max) {
        // We don't need to go to lower level, value from subtree ref can be used instead.
        auto agg = INIT_AGGRES;
        agg.copy_from(ref);
        result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    } else {
        result.reset(new NBTreeSBlockAggregator(bstore_, ref.addr, begin_, end_));
    }
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}


// //////////////////////////// //
// NBTreeSBlockCandlesticksIter //
// //////////////////////////// //

class NBTreeSBlockCandlesticsIter : public NBTreeSBlockIteratorBase<NBTreeAggregationResult> {
    NBTreeCandlestickHint hint_;
public:
    NBTreeSBlockCandlesticsIter(std::shared_ptr<BlockStore> bstore,
                                NBTreeSuperblock const& sblock,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                NBTreeCandlestickHint hint)
        : NBTreeSBlockIteratorBase<NBTreeAggregationResult>(bstore, sblock, begin, end)
        , hint_(hint)
    {
    }

    NBTreeSBlockCandlesticsIter(std::shared_ptr<BlockStore> bstore,
                                LogicAddr addr,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                NBTreeCandlestickHint hint)
        : NBTreeSBlockIteratorBase<NBTreeAggregationResult>(bstore, addr, begin, end)
        , hint_(hint)
    {
    }
    virtual std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> make_leaf_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> make_superblock_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) override;
};


std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> NBTreeSBlockCandlesticsIter::make_leaf_iterator(const SubtreeRef &ref) {
    auto agg = INIT_AGGRES;
    agg.copy_from(ref);
    std::unique_ptr<NBTreeAggregator> result;
    result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, std::unique_ptr<NBTreeAggregator>> NBTreeSBlockCandlesticsIter::make_superblock_iterator(const SubtreeRef &ref) {
    aku_Timestamp min = std::min(begin_, end_);
    aku_Timestamp max = std::max(begin_, end_);
    aku_Timestamp delta = max - min;
    std::unique_ptr<NBTreeAggregator> result;
    if (min < ref.begin && ref.end < max && hint_.min_delta > delta) {
        // We don't need to go to lower level, value from subtree ref can be used instead.
        auto agg = INIT_AGGRES;
        agg.copy_from(ref);
        result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    } else {
        result.reset(new NBTreeSBlockCandlesticsIter(bstore_, ref.addr, begin_, end_, hint_));
    }
    return std::make_tuple(AKU_SUCCESS, std::move(result));

}

std::tuple<aku_Status, size_t> NBTreeSBlockCandlesticsIter::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
    if (!fsm_pos_ ) {
        aku_Status status = AKU_SUCCESS;
        status = init();
        if (status != AKU_SUCCESS) {
            return std::make_pair(status, 0ul);
        }
        fsm_pos_++;
    }
    return iter(destts, destval, size);
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
    assert((prev == EMPTY_ADDR && fanout_index == 0) || (prev != EMPTY_ADDR && fanout_index > 0));
    SubtreeRef* subtree = subtree_cast(block_->get_data());
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
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    prev_ = subtree->addr;
    fanout_index_ = subtree->fanout_index;
}

size_t NBTreeLeaf::_get_uncommitted_size() const {
    return static_cast<size_t>(writer_.get_write_index());
}

SubtreeRef const* NBTreeLeaf::get_leafmeta() const {
    return subtree_cast(block_->get_data());
}

size_t NBTreeLeaf::nelements() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_data());
    return subtree->count;
}

u16 NBTreeLeaf::get_fanout() const {
    return fanout_index_;
}

aku_ParamId NBTreeLeaf::get_id() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_data());
    return subtree->id;
}

std::tuple<aku_Timestamp, aku_Timestamp> NBTreeLeaf::get_timestamps() const {
    SubtreeRef const* subtree = subtree_cast(block_->get_data());
    return std::make_tuple(subtree->begin, subtree->end);
}

LogicAddr NBTreeLeaf::get_prev_addr() const {
    // Should be set correctly no metter how NBTreeLeaf was created.
    return prev_;
}


aku_Status NBTreeLeaf::read_all(std::vector<aku_Timestamp>* timestamps,
                                std::vector<double>* values) const
{
    int windex = writer_.get_write_index();
    DataBlockReader reader(block_->get_data() + sizeof(SubtreeRef), block_->get_size());
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
    size_t size = writer_.commit();
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    subtree->payload_size = static_cast<u16>(size);
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
    subtree->fanout_index = fanout_index_;
    // Compute checksum
    subtree->checksum = bstore->checksum(block_->get_data() + sizeof(SubtreeRef), size);
    return bstore->append_block(block_);
}


std::unique_ptr<NBTreeIterator> NBTreeLeaf::range(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<NBTreeIterator> it;
    it.reset(new NBTreeLeafIterator(begin, end, *this));
    return std::move(it);
}

std::unique_ptr<NBTreeAggregator> NBTreeLeaf::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    std::unique_ptr<NBTreeAggregator> it;
    it.reset(new NBTreeLeafAggregator(begin, end, *this));
    return std::move(it);
}

std::unique_ptr<NBTreeAggregator> NBTreeLeaf::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    AKU_UNUSED(hint);
    auto agg = INIT_AGGRES;
    SubtreeRef* subtree = subtree_cast(block_->get_data());
    agg.copy_from(*subtree);
    std::unique_ptr<NBTreeAggregator> result;
    NBTreeAggregator::Direction dir = begin < end ? NBTreeAggregator::Direction::FORWARD : NBTreeAggregator::Direction::BACKWARD;
    result.reset(new ValueAggregator(subtree->end, agg, dir));
    return std::move(result);
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
    : block_(std::make_shared<Block>())
    , id_(id)
    , write_pos_(0)
    , fanout_index_(fanout)
    , level_(lvl)
    , prev_(prev)
    , immutable_(false)
{
}

NBTreeSuperblock::NBTreeSuperblock(std::shared_ptr<Block> block)
    : block_(block)
    , immutable_(true)
{
    // Use zero-copy here.
    SubtreeRef const* ref = subtree_cast(block->get_data());
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    write_pos_ = ref->payload_size;
    level_ = ref->level;
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
    SubtreeRef const* ref = subtree_cast(block->get_data());
    id_ = ref->id;
    fanout_index_ = ref->fanout_index;
    prev_ = ref->addr;
    level_ = ref->level;
    write_pos_ = ref->payload_size;
    if (remove_last && write_pos_ != 0) {
        // We can't use zero-copy here because `block` belongs to other node.
        write_pos_--;
        memcpy(block_->get_data(), block->get_data(), AKU_BLOCK_SIZE);
    }
    else {
        // Zero copy
        block_ = block;
    }
}

SubtreeRef const* NBTreeSuperblock::get_sblockmeta() const {
    SubtreeRef const* pref = subtree_cast(block_->get_data());
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
    return subtree_cast(block_->get_data())->addr;
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
    if (immutable_) {
        return std::make_tuple(AKU_EBAD_DATA, EMPTY_ADDR);
    }
    SubtreeRef* backref = subtree_cast(block_->get_data());
    if (fanout_index_ != 0) {
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore, prev_);
        if (status == AKU_EUNAVAILABLE) {
            // Previous root was deleted due to retention policy
            backref->addr = EMPTY_ADDR;
        } else if (status !=  AKU_SUCCESS) {
            // Some other error!
            return std::make_tuple(status, EMPTY_ADDR);
        } else {
            // Everything is OK
            NBTreeSuperblock subtree(block);
            status = init_subtree_from_subtree(subtree, *backref);
            if (status != AKU_SUCCESS) {
                return std::make_tuple(status, EMPTY_ADDR);
            }
            backref->addr = prev_;
        }
    } else {
        backref->addr = EMPTY_ADDR;
    }
    // This fields should be rewrited to store node's own information
    backref->payload_size = static_cast<u16>(write_pos_);
    assert(backref->payload_size + sizeof(SubtreeRef) < AKU_BLOCK_SIZE);
    backref->fanout_index = fanout_index_;
    backref->id = id_;
    backref->level = level_;
    backref->version = AKUMULI_VERSION;
    // add checksum
    backref->checksum = bstore->checksum(block_->get_data() + sizeof(SubtreeRef), backref->payload_size);
    return bstore->append_block(block_);
}

bool NBTreeSuperblock::is_full() const {
    return write_pos_ >= AKU_NBTREE_FANOUT;
}

aku_Status NBTreeSuperblock::read_all(std::vector<SubtreeRef>* refs) const {
    SubtreeRef const* ref = subtree_cast(block_->get_data());
    for(u32 ix = 0u; ix < write_pos_; ix++) {
        auto p = ref + 1 + ix;
        refs->push_back(*p);
    }
    return AKU_SUCCESS;
}

std::tuple<aku_Timestamp, aku_Timestamp> NBTreeSuperblock::get_timestamps() const {
    SubtreeRef const* pref = subtree_cast(block_->get_data());
    return std::tie(pref->begin, pref->end);
}

std::unique_ptr<NBTreeIterator> NBTreeSuperblock::search(aku_Timestamp begin,
                                                         aku_Timestamp end,
                                                         std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<NBTreeIterator> result;
    result.reset(new NBTreeSBlockIterator(bstore, *this, begin, end));
    return std::move(result);
}

std::unique_ptr<NBTreeAggregator> NBTreeSuperblock::aggregate(aku_Timestamp begin,
                                                            aku_Timestamp end,
                                                            std::shared_ptr<BlockStore> bstore) const
{
    std::unique_ptr<NBTreeAggregator> result;
    result.reset(new NBTreeSBlockAggregator(bstore, *this, begin, end));
    return std::move(result);
}

std::unique_ptr<NBTreeAggregator> NBTreeSuperblock::candlesticks(aku_Timestamp begin, aku_Timestamp end,
                                                                 std::shared_ptr<BlockStore> bstore,
                                                                 NBTreeCandlestickHint hint) const
{
    std::unique_ptr<NBTreeAggregator> result;
    result.reset(new NBTreeSBlockCandlesticsIter(bstore, *this, begin, end, hint));
    return std::move(result);
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
    std::shared_ptr<NBTreeLeaf> leaf_;
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
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            } else {
                auto psubtree = subtree_cast(block->get_data());
                fanout_index_ = psubtree->fanout_index + 1;
                if (fanout_index_ == AKU_NBTREE_FANOUT) {
                    fanout_index_ = 0;
                    last_ = EMPTY_ADDR;
                }
            }
        }
        reset_leaf();
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
        leaf_.reset(new NBTreeLeaf(id_, last_, fanout_index_));
    }

    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value);
    virtual std::tuple<bool, LogicAddr> append(const SubtreeRef &pl);
    virtual std::tuple<bool, LogicAddr> commit(bool final);
    virtual std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end) const;
    virtual std::unique_ptr<NBTreeAggregator> aggregate(aku_Timestamp begin, aku_Timestamp end) const;
    virtual std::unique_ptr<NBTreeAggregator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const;
    virtual bool is_dirty() const;
    virtual void debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat) const override;
};


static void dump_subtree_ref(std::ostream& stream,
                             SubtreeRef const* ref,
                             LogicAddr prev_addr,
                             int base_indent,
                             std::function<std::string(aku_Timestamp)> tsformat)
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
    if (ref->level == 0) {
        stream << tag("type")     << "Leaf"                       << "</type>\n";
    } else {
        stream << tag("type")     << "Superblock"                 << "</type>\n";
    }
    stream << tag("id")           << ref->id                      << "</id>\n";
    stream << tag("prev_addr")    << afmt(prev_addr)              << "</prev_addr>\n";
    stream << tag("begin")        << tsformat(ref->begin)         << "</begin>\n";
    stream << tag("end")          << tsformat(ref->end)           << "</end>\n";
    stream << tag("count")        << ref->count                   << "</count>\n";
    stream << tag("min")          << ref->min                     << "</min>\n";
    stream << tag("min_time")     << tsformat(ref->min_time)      << "</min_time>\n";
    stream << tag("max")          << ref->max                     << "</max>\n";
    stream << tag("max_time")     << tsformat(ref->max_time)      << "</max_time>\n";
    stream << tag("sum")          << ref->sum                     << "</sum>\n";
    stream << tag("first")        << ref->first                   << "</first>\n";
    stream << tag("last")         << ref->last                    << "</last>\n";
    stream << tag("version")      << ref->version                 << "</version>\n";
    stream << tag("level")        << ref->level                   << "</level>\n";
    stream << tag("payload_size") << ref->payload_size            << "</payload_size>\n";
    stream << tag("fanout_index") << ref->fanout_index            << "</fanout_index>\n";
    stream << tag("checksum")     << ref->checksum                << "</checksum>\n";
}


void NBTreeLeafExtent::debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat) const {
    SubtreeRef const* ref = leaf_->get_leafmeta();
    stream << std::string(static_cast<size_t>(base_indent), '\t') <<  "<node>\n";
    dump_subtree_ref(stream, ref, leaf_->get_prev_addr(), base_indent + 1, tsformat);
    stream << std::string(static_cast<size_t>(base_indent), '\t') << "</node>\n";
}

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
    SubtreeRef payload = INIT_SUBTREE_REF;
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
        if (!final || roots_collection->_get_roots().size() > next_level) {
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

std::unique_ptr<NBTreeAggregator> NBTreeLeafExtent::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    return std::move(leaf_->aggregate(begin, end));
}

std::unique_ptr<NBTreeAggregator> NBTreeLeafExtent::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    return std::move(leaf_->candlesticks(begin, end, hint));
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
            std::tie(status, block) = read_and_check(bstore_, addr);
            if (status  == AKU_EUNAVAILABLE) {
                addr = EMPTY_ADDR;
            } else if (status != AKU_SUCCESS) {
                AKU_PANIC("Invalid argument, " + StatusUtil::str(status));
            } else {
                auto psubtree = subtree_cast(block->get_data());
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
    virtual std::unique_ptr<NBTreeAggregator> aggregate(aku_Timestamp begin, aku_Timestamp end) const;
    virtual std::unique_ptr<NBTreeAggregator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const;
    virtual bool is_dirty() const;
    virtual void debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat) const override;
};

void NBTreeSBlockExtent::debug_dump(std::ostream& stream, int base_indent, std::function<std::string(aku_Timestamp)> tsformat) const {
    SubtreeRef const* ref = curr_->get_sblockmeta();
    stream << std::string(static_cast<size_t>(base_indent), '\t') <<  "<node>\n";
    dump_subtree_ref(stream, ref, curr_->get_prev_addr(), base_indent + 1, tsformat);

    std::vector<SubtreeRef> refs;
    aku_Status status = curr_->read_all(&refs);
    if (status != AKU_SUCCESS) {
        // TODO: error message
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
            auto subtreeref = reinterpret_cast<SubtreeRef*>(block->get_data());
            u16 level = subtreeref->level;
            if (level == 0) {
                // leaf node
                NBTreeLeaf leaf(block);
                SubtreeRef const* ref = leaf.get_leafmeta();
                dump_subtree_ref(stream, ref, leaf.get_prev_addr(), indent, tsformat);
            } else {
                // superblock
                NBTreeSuperblock sblock(block);
                SubtreeRef const* ref = sblock.get_sblockmeta();
                dump_subtree_ref(stream, ref, sblock.get_prev_addr(), indent, tsformat);
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

std::unique_ptr<NBTreeIterator> NBTreeSBlockExtent::search(aku_Timestamp begin, aku_Timestamp end) const {
    return std::move(curr_->search(begin, end, bstore_));
}

std::unique_ptr<NBTreeAggregator> NBTreeSBlockExtent::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    return std::move(curr_->aggregate(begin, end, bstore_));
}

std::unique_ptr<NBTreeAggregator> NBTreeSBlockExtent::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    return std::move(curr_->candlesticks(begin, end, bstore_, hint));
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
        std::tie(status, block) = read_and_check(bstore, refs[i].addr);
        if (status == AKU_EUNAVAILABLE) {
            // block was deleted due to retention.
            Logger::msg(AKU_LOG_INFO, "Block " + std::to_string(refs[i].addr));
        } else if (status == AKU_SUCCESS) {
            SubtreeRef out = INIT_SUBTREE_REF;
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
    , last_(0ull)
    , rescue_points_(std::move(addresses))
    , initialized_(false)
    , write_count_(0ul)
{
    if (rescue_points_.size() >= std::numeric_limits<u16>::max()) {
        AKU_PANIC("Tree depth is too large");
    }
}

void NBTreeExtentsList::force_init() {
    UniqueLock lock(lock_);
    if (!initialized_) {
        init();
    }
}

size_t NBTreeExtentsList::_get_uncommitted_size() const {
    SharedLock lock(lock_);
    if (!extents_.empty()) {
        auto leaf = dynamic_cast<NBTreeLeafExtent const*>(extents_.front().get());
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

NBTreeAppendResult NBTreeExtentsList::append(aku_Timestamp ts, double value) {
    UniqueLock lock(lock_);  // NOTE: NBTreeExtentsList::append(subtree) can be called from here
                             //       recursively (maybe even many times).
    if (!initialized_) {
        AKU_PANIC("NB+tree not imitialized");
    }
    if (ts < last_) {
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
    bool parent_saved = false;
    LogicAddr addr = EMPTY_ADDR;
    std::tie(parent_saved, addr) = extents_.front()->append(ts, value);
    if (addr != EMPTY_ADDR) {
        if (rescue_points_.size() > 0) {
            rescue_points_.at(0) = addr;
        } else {
            rescue_points_.push_back(addr);
        }
        return NBTreeAppendResult::OK_FLUSH_NEEDED;
    }
    return NBTreeAppendResult::OK;
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
            // FIXME: handle obsolete trees correctly!
            Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Obsolete tree handling not implemented");
            initialized_ = false;
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
    // Restore `last_`
    if (extents_.size()) {
        auto it = extents_.back()->search(AKU_MAX_TIMESTAMP, 0);
        aku_Timestamp ts;
        double val;
        aku_Status status;
        size_t nread;
        std::tie(status, nread) = it->read(&ts, &val, 1);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't restore last timestamp from tree");
            AKU_PANIC("Can't restore last timestamp from tree");
        }
        last_ = ts;
    }
}

static void create_empty_extents(std::shared_ptr<NBTreeExtentsList> self,
                                 std::shared_ptr<BlockStore> bstore,
                                 aku_ParamId id,
                                 size_t nlevels,
                                 std::deque<std::unique_ptr<NBTreeExtent>>* extents)
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
    std::vector<LogicAddr> rescue_points(rescue_points_.begin(), rescue_points_.end());

    // Construct roots using CoW
    if (rescue_points.size() < 2) {
        // All data was lost.
        create_empty_extents(shared_from_this(), bstore_, id_, 1, &extents_);
    } else {
        // Init `extents_` to make `append` functions work.
        create_empty_extents(shared_from_this(), bstore_, id_, rescue_points.size(), &extents_);

        int i = static_cast<int>(rescue_points.size());
        while (i --> 0) {
            std::vector<SubtreeRef> refs;
            if (rescue_points.at(static_cast<size_t>(i)) != EMPTY_ADDR) {
                continue;
            } else if (i == 1) {
                // Resestore this level from last saved leaf node.
                auto leaf_addr = rescue_points.front();
                assert(rescue_points.at(1) == EMPTY_ADDR);
                // Recover all leaf nodes in reverse order.
                while(leaf_addr != EMPTY_ADDR) {
                    aku_Status status;
                    std::shared_ptr<Block> block;
                    std::tie(status, block) = read_and_check(bstore_, leaf_addr);
                    if (status != AKU_SUCCESS) {
                        // Leaf node was deleted because of retention process,
                        // we should stop recovery process.
                        break;
                    }
                    NBTreeLeaf leaf(block);
                    SubtreeRef ref = INIT_SUBTREE_REF;
                    status = init_subtree_from_leaf(leaf, ref);
                    if (status != AKU_SUCCESS) {
                        Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't summarize leaf node at " +
                                                   std::to_string(leaf_addr) + " error: " +
                                                   StatusUtil::str(status));
                    }
                    ref.addr = leaf_addr;
                    leaf_addr = leaf.get_prev_addr();
                    refs.push_back(ref);
                }
            } else if (i > 1) {
                // resestore this level from last saved inner node
                auto inner_addr = rescue_points.at(static_cast<size_t>(i - 1));
                // Recover all inner nodes in reverse order.
                while(inner_addr != EMPTY_ADDR) {
                    aku_Status status;
                    std::shared_ptr<Block> block;
                    std::tie(status, block) = read_and_check(bstore_, inner_addr);
                    if (status != AKU_SUCCESS) {
                        // Leaf node was deleted because of retention process,
                        // we should stop recovery process.
                        break;
                    }
                    NBTreeSuperblock sblock(block);
                    SubtreeRef ref = INIT_SUBTREE_REF;
                    status = init_subtree_from_subtree(sblock, ref);
                    if (status != AKU_SUCCESS) {
                        Logger::msg(AKU_LOG_ERROR, std::to_string(id_) + " Can't summarize inner node at " +
                                                   std::to_string(inner_addr) + " error: " +
                                                   StatusUtil::str(status));
                    }
                    ref.addr = inner_addr;
                    inner_addr = sblock.get_prev_addr();
                    refs.push_back(ref);
                }
                rescue_points.at(static_cast<size_t>(i - 1)) = EMPTY_ADDR;
            }
            // Insert all nodes in direct order
            for(auto it = refs.rbegin(); it < refs.rend(); it++) {
                append(*it);  // There is no need to check return value.
            }
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

std::unique_ptr<NBTreeIterator> NBTreeExtentsList::search(aku_Timestamp begin, aku_Timestamp end) const {
    SharedLock lock(lock_);
    if (!initialized_) {
        AKU_PANIC("NB+tree not imitialized");
    }
    std::vector<std::unique_ptr<NBTreeIterator>> iterators;
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
    std::unique_ptr<NBTreeIterator> concat;
    concat.reset(new IteratorConcat(std::move(iterators)));
    return concat;
}

std::unique_ptr<NBTreeAggregator> NBTreeExtentsList::aggregate(aku_Timestamp begin, aku_Timestamp end) const {
    SharedLock lock(lock_);
    if (!initialized_) {
        AKU_PANIC("NB+tree not imitialized");
    }
    std::vector<std::unique_ptr<NBTreeAggregator>> iterators;
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
    std::unique_ptr<NBTreeAggregator> concat;
    concat.reset(new IteratorAggregate(std::move(iterators)));
    return concat;

}


std::unique_ptr<NBTreeAggregator> NBTreeExtentsList::candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const {
    SharedLock lock(lock_);
    if (!initialized_) {
        AKU_PANIC("NB+tree not imitialized");
    }
    std::vector<std::unique_ptr<NBTreeAggregator>> iterators;
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
    std::unique_ptr<NBTreeAggregator> concat;
    concat.reset(new IteratorAggregate(std::move(iterators)));
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
    auto ref = reinterpret_cast<SubtreeRef const*>(block->get_data());
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
