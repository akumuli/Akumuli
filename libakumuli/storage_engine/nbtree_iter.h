#pragma once

// C++ headers
#include <deque>
#include <algorithm>
#include <vector>

// App headers
#include "nbtree.h"
#include "nbtree_def.h"
#include "blockstore.h"
#include "compression.h"
#include "operators/operator.h"
#include "log_iface.h"
#include "status_util.h"

namespace Akumuli {
namespace StorageEngine {


// ////////////// //
// Free Functions //
// ////////////// //

namespace {

inline SubtreeRef* subtree_cast(u8* p) {
    return reinterpret_cast<SubtreeRef*>(p);
}

inline SubtreeRef const* subtree_cast(u8 const* p) {
    return reinterpret_cast<SubtreeRef const*>(p);
}

}

std::tuple<aku_Status, std::shared_ptr<Block>> read_and_check(std::shared_ptr<BlockStore> bstore, LogicAddr curr);

/** QueryOperator implementation for leaf node.
  * This is very basic. All node's data is copied to
  * the internal buffer by c-tor.
  */
struct NBTreeLeafIterator : RealValuedOperator {

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

    template<class LeafT>
    NBTreeLeafIterator(aku_Timestamp begin, aku_Timestamp end, LeafT const& node, bool delay_init=false)
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

    template<class LeafT>
    void init(LeafT const& node) {
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


// ///////////////////// //
// NBTreeLeafGroupFilter //
// ///////////////////// //

class NBTreeGroupAggregateFilter : public AggregateOperator {
    AggregateFilter filter_;
    std::unique_ptr<AggregateOperator> iter_;
public:
    NBTreeGroupAggregateFilter(const AggregateFilter& filter, std::unique_ptr<AggregateOperator>&& iter)
        : filter_(filter)
        , iter_(std::move(iter))
    {
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size) {
        // copy data to the buffer
        size_t i = 0;
        while (i < size) {
            AggregationResult agg;
            aku_Timestamp ts;
            size_t outsz;
            aku_Status status;
            std::tie(status, outsz) = iter_->read(&ts, &agg, 1);
            if (status == AKU_SUCCESS || status == AKU_ENO_DATA) {
                if (filter_.match(agg)) {
                    destts[i] = ts;
                    destval[i] = agg;
                    i++;
                }
                if (status == AKU_ENO_DATA || outsz == 0) {
                    // Stop iteration
                    break;
                }
            } else {
                // Error
                return std::make_tuple(status, 0);
            }
        }
        return std::make_tuple(AKU_SUCCESS, i);
    }

    virtual Direction get_direction() {
        return iter_->get_direction();
    }
};


// //////////////////////// //
// Superblock Iterator Base //
// //////////////////////// //

template<class TVal>
struct NBTreeSBlockIteratorBase : SeriesOperator<TVal> {
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
    std::unique_ptr<SeriesOperator<TVal>> iter_;
    u32 fsm_pos_;
    i32 refs_pos_;

    typedef std::unique_ptr<SeriesOperator<TVal>> TIter;
    typedef typename SeriesOperator<TVal>::Direction Direction;

    //! Return true if referenced subtree in [begin, end) range.
    //! @note Begin should be less then end.
    static bool subtree_in_range(SubtreeRef const& ref, aku_Timestamp begin, aku_Timestamp end) {
        if (ref.end < begin || end < ref.begin) {
            return false;
        }
        return true;
    }

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
        if (!bstore_->exists(ref.addr)) {
            return std::make_tuple(AKU_EUNAVAILABLE, std::move(empty));
        }
        if (!subtree_in_range(ref, min, max)) {
            // Subtree not in [begin_, end_) range. Proceed to next.
            result = std::make_tuple(AKU_ENOT_FOUND, std::move(empty));
        } else if (ref.type == NBTreeBlockType::LEAF) {
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
            if (status == AKU_ENO_DATA ||
               (status == AKU_EUNAVAILABLE && get_direction() == Direction::FORWARD)) {
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


// //////////////////////////// //
// NBTreeSBlockCandlesticksIter //
// //////////////////////////// //

class NBTreeSBlockCandlesticsIter : public NBTreeSBlockIteratorBase<AggregationResult> {
    NBTreeCandlestickHint hint_;
public:
    NBTreeSBlockCandlesticsIter(std::shared_ptr<BlockStore> bstore,
                                NBTreeSuperblock const& sblock,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                NBTreeCandlestickHint hint)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, sblock, begin, end)
        , hint_(hint)
    {
    }

    NBTreeSBlockCandlesticsIter(std::shared_ptr<BlockStore> bstore,
                                LogicAddr addr,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                NBTreeCandlestickHint hint)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, addr, begin, end)
        , hint_(hint)
    {
    }
    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_leaf_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_superblock_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size) override;
};



// ////////////////////// //
// NBTreeSBlockAggregator //
// ////////////////////// //

/** Aggregator that returns precomputed value.
  * Value should be set in c-tor.
  */
class ValueAggregator : public AggregateOperator {
    aku_Timestamp ts_;
    AggregationResult value_;
    Direction dir_;
    bool used_;
public:
    ValueAggregator(aku_Timestamp ts, AggregationResult value, Direction dir)
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

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size) override;
    virtual Direction get_direction() override;
};

/** Superblock aggregator (iterator that computes different aggregates e.g. min/max/avg/sum).
  * Uses metadata stored in superblocks in some cases.
  */
class NBTreeSBlockAggregator : public NBTreeSBlockIteratorBase<AggregationResult> {

    bool leftmost_leaf_found_;

public:
    NBTreeSBlockAggregator(std::shared_ptr<BlockStore> bstore,
                           NBTreeSuperblock const& sblock,
                           aku_Timestamp begin,
                           aku_Timestamp end)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, sblock, begin, end)
        , leftmost_leaf_found_(false)
    {
    }

    NBTreeSBlockAggregator(std::shared_ptr<BlockStore> bstore,
                           LogicAddr addr,
                           aku_Timestamp begin,
                           aku_Timestamp end)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, addr, begin, end)
        , leftmost_leaf_found_(false)
    {
    }
    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_leaf_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_superblock_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size) override;
};

// ///////////////////////// //
// NBTreeLeafGroupAggregator //
// ///////////////////////// //

class NBTreeLeafGroupAggregator : public AggregateOperator {
    NBTreeLeafIterator iter_;
    bool enable_cached_metadata_;
    SubtreeRef metacache_;
    aku_Timestamp begin_;
    aku_Timestamp end_;
    aku_Timestamp step_;
public:
    template<class LeafT>
    NBTreeLeafGroupAggregator(aku_Timestamp begin, aku_Timestamp end, u64 step, LeafT const& node)
        : iter_(begin, end, node, true)
        , enable_cached_metadata_(false)
        , metacache_(INIT_SUBTREE_REF)
        , begin_(begin)
        , end_(end)
        , step_(step)
    {
        aku_Timestamp nodemin, nodemax;
        std::tie(nodemin, nodemax) = node.get_timestamps();
        if (begin < end) {
            auto a = (nodemin - begin) / step;
            auto b = (nodemax - begin) / step;
            if (a == b && nodemin >= begin && nodemax < end) {
                // Leaf totally inside one step range, we can use metadata.
                metacache_ = *node.get_leafmeta();
                enable_cached_metadata_ = true;
            } else {
                // Otherwise we need to compute aggregate from subset of leaf's values.
                iter_.init(node);
            }
        } else {
            auto a = (begin - nodemin) / step;
            auto b = (begin - nodemax) / step;
            if (a == b && nodemax <= begin && nodemin > end) {
                // Leaf totally inside one step range, we can use metadata.
                metacache_ = *node.get_leafmeta();
                enable_cached_metadata_ = true;
            } else {
                // Otherwise we need to compute aggregate from subset of leaf's values.
                iter_.init(node);
            }
        }
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destxs, size_t size);
    virtual Direction get_direction();
};


/** Superblock aggregator (iterator that computes different aggregates e.g. min/max/avg/sum).
  * Uses metadata stored in superblocks in some cases.
  */
class NBTreeSBlockGroupAggregator : public NBTreeSBlockIteratorBase<AggregationResult> {
    typedef std::vector<AggregationResult> ReadBuffer;
    u64 step_;
    ReadBuffer rdbuf_;
    u32 rdpos_;
    bool done_;
    enum {
        RDBUF_SIZE = 0x100
    };
public:
    NBTreeSBlockGroupAggregator(std::shared_ptr<BlockStore> bstore,
                                NBTreeSuperblock const& sblock,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                u64 step)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, sblock, begin, end)
        , step_(step)
        , rdpos_(0)
        , done_(false)
    {
    }

    NBTreeSBlockGroupAggregator(std::shared_ptr<BlockStore> bstore,
                                LogicAddr addr,
                                aku_Timestamp begin,
                                aku_Timestamp end,
                                u64 step)
        : NBTreeSBlockIteratorBase<AggregationResult>(bstore, addr, begin, end)
        , step_(step)
        , rdpos_(0)
        , done_(false)
    {
    }

    //! Return true if `rdbuf_` is not empty and have some data to read.
    bool can_read() const {
        return rdpos_ < rdbuf_.size();
    }

    //! Return number of elements in rdbuf_ available for reading
    u32 elements_in_rdbuf() const {
        return static_cast<u32>(rdbuf_.size()) - rdpos_;  // Safe to cast because rdbuf_.size() <= RDBUF_SIZE
    }

    /**
     * @brief Copy as much elements as possible to the dest arrays.
     * @param desttx timestamps array
     * @param destxs values array
     * @param size size of both arrays
     * @return number of elements copied
     */
    std::tuple<aku_Status, size_t> copy_to(aku_Timestamp* desttx, AggregationResult* destxs, size_t size) {
        aku_Status status = AKU_SUCCESS;
        size_t copied = 0;
        while (status == AKU_SUCCESS && size > 0) {
            size_t n = elements_in_rdbuf();
            if (!done_) {
                if (n < 2) {
                    status = refill_read_buffer();
                    if (status == AKU_ENO_DATA && can_read()) {
                        status = AKU_SUCCESS;
                    }
                    continue;
                }
                // We can copy last element of the rdbuf_ to the output only if all
                // iterators were consumed! Otherwise invariant will be broken.
                n--;
            } else {
                if (n == 0) {
                    status = AKU_ENO_DATA;
                    break;
                }
            }
            //
            // Copy elements
            auto tocopy = std::min(n, size);
            for (size_t i = 0; i < tocopy; i++) {
                auto const& bottom = rdbuf_.at(rdpos_);
                rdpos_++;
                *desttx++ = bottom._begin;
                *destxs++ = bottom;
                size--;
            }
            copied += tocopy;
        }
        return std::make_tuple(status, copied);
    }

    /**
     * @brief Refils read buffer.
     * @return AKU_SUCCESS on success, AKU_ENO_DATA if there is no more data to read, error code on error
     */
    aku_Status refill_read_buffer() {
        aku_Status status = AKU_ENO_DATA;
        u32 pos_ = 0;

        if (!rdbuf_.empty()) {
            auto tail = rdbuf_.back();  // the last element should be saved because it is possible that
                                        // it's not full (part of the range contained in first iterator
                                        // and another part in second iterator or even in more than one
                                        // iterators).
            rdbuf_.clear();
            rdbuf_.resize(RDBUF_SIZE, INIT_AGGRES);
            rdpos_ = 0;
            rdbuf_.at(0) = tail;
            pos_ = 1;
        } else {
            rdbuf_.clear();
            rdbuf_.resize(RDBUF_SIZE, INIT_AGGRES);
            rdpos_ = 0;
        }

        while(true) {
            if (!iter_) {
                std::tie(status, iter_) = get_next_iter();
                if (status == AKU_ENOT_FOUND || status == AKU_EUNAVAILABLE) {
                    // Subtree exists but doesn't contains values from begin-end timerange or
                    // entire subtree was deleted
                    Logger::msg(AKU_LOG_TRACE, "Can't open next iterator because " + StatusUtil::str(status));
                    continue;
                } else if (status != AKU_SUCCESS) {
                    // We're out of iterators and should stop.
                    done_ = true;
                    break;
                }
            }
            size_t size = rdbuf_.size() - pos_;
            if (size == 0) {
                break;
            }
            std::array<AggregationResult, RDBUF_SIZE> outxs;
            std::array<aku_Timestamp, RDBUF_SIZE>           outts;
            u32 outsz;
            std::tie(status, outsz) = iter_->read(outts.data(), outxs.data(), size);
            if (outsz != 0) {
                if (pos_ > 0) {
                    auto const& last  = rdbuf_.at(pos_ - 1);
                    auto const& first = outxs.front();
                    aku_Timestamp lastts = begin_ < end_ ? last._begin - begin_
                                                         : begin_ - last._begin;
                    aku_Timestamp firstts = begin_ < end_ ? first._begin - begin_
                                                          : begin_ - first._begin;
                    auto lastbin = lastts / step_;
                    auto firstbin = firstts / step_;

                    if (lastbin == firstbin) {
                        pos_--;
                    }
                }
                for (size_t ix = 0; ix < outsz; ix++) {
                    rdbuf_.at(pos_).combine(outxs.at(ix));
                    const auto newdelta = rdbuf_.at(pos_)._end - rdbuf_.at(pos_)._begin;
                    if (newdelta > step_) {
                        assert(newdelta <= step_);
                    }
                    pos_++;
                }
            }
            if (status == AKU_ENO_DATA) {
                iter_.reset();
                continue;
            }
            if (status != AKU_SUCCESS) {
                size = 0;
                break;
            }
        }
        rdbuf_.resize(pos_);
        return status;
    }

    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_leaf_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> make_superblock_iterator(const SubtreeRef &ref) override;
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size) override;
};


// ////////////////////// //
// class NBTreeLeafFilter //
// ////////////////////// //

/** Filtering operator for the leaf node.
  * Returns all data-points that match the ValueFilter
  */
struct NBTreeLeafFilter : RealValuedOperator {

    //! Starting timestamp
    aku_Timestamp              begin_;
    //! Final timestamp
    aku_Timestamp              end_;
    //! Timestamps
    std::vector<aku_Timestamp> tsbuf_;
    //! Values
    std::vector<double>        xsbuf_;
    //! Status of the iterator initialization process
    aku_Status                 status_;
    //! ValueFilter
    ValueFilter                filter_;
    //! Read cursor position
    size_t                     pos_;

    NBTreeLeafFilter(aku_Status status)
        : begin_()
        , end_()
        , status_(status)
        , pos_()
    {
    }

    template<class LeafT>
    NBTreeLeafFilter(aku_Timestamp begin,
                     aku_Timestamp end,
                     const ValueFilter& filter,
                     const LeafT& node,
                     bool delay_init=false)
        : begin_(begin)
        , end_(end)
        , status_(AKU_ENO_DATA)
        , filter_(filter)
        , pos_()
    {
        if (!delay_init) {
            init(node);
        }
    }

    template<class LeafT>
    void init(LeafT const& node) {
        aku_Timestamp min = std::min(begin_, end_);
        aku_Timestamp max = std::max(begin_, end_);
        aku_Timestamp nb, ne;
        std::tie(nb, ne) = node.get_timestamps();
        if (max < nb || ne < min) {
            status_ = AKU_ENO_DATA;
            return;
        }
        std::vector<aku_Timestamp> tss;
        std::vector<double>        xss;
        status_ = node.read_all(&tss, &xss);
        ssize_t from = 0, to = 0;
        if (status_ == AKU_SUCCESS) {
            if (begin_ < end_) {
                // FWD direction
                auto it_begin = std::lower_bound(tss.begin(), tss.end(), begin_);
                if (it_begin != tss.end()) {
                    from = std::distance(tss.begin(), it_begin);
                } else {
                    from = 0;
                    assert(tss.front() > begin_);
                }

                auto it_end = std::lower_bound(tss.begin(), tss.end(), end_);
                to = std::distance(tss.begin(), it_end);

                for (ssize_t ix = from; ix < to; ix++){
                    if (filter_.match(xss[ix])) {
                        tsbuf_.push_back(tss[ix]);
                        xsbuf_.push_back(xss[ix]);
                    }
                }
            } else {
                // BWD direction
                auto it_begin = std::lower_bound(tss.begin(), tss.end(), begin_);
                if (it_begin != tss.end()) {
                    from = std::distance(tss.begin(), it_begin);
                }
                else {
                    from = tss.size() - 1;
                }

                auto it_end = std::upper_bound(tss.begin(), tss.end(), end_);
                to = std::distance(tss.begin(), it_end);

                for (ssize_t ix = from; ix >= to; ix--){
                    if (filter_.match(xss[ix])) {
                        tsbuf_.push_back(tss[ix]);
                        xsbuf_.push_back(xss[ix]);
                    }
                }
            }
        }
    }

    size_t get_size() const {
        return static_cast<size_t>(tsbuf_.size());
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size);
    virtual Direction get_direction();
};

// ////////////////////////// //
// class NBTreeSBlockIterator //
// ////////////////////////// //

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
        assert(ref.type == NBTreeBlockType::LEAF);
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, ref.addr);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, std::unique_ptr<RealValuedOperator>());
        }
        auto blockref = subtree_cast(block->get_cdata());
        assert(blockref->type == ref.type);
        AKU_UNUSED(blockref);
        NBTreeLeaf leaf(block);
        std::unique_ptr<RealValuedOperator> result;
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


// /////////////////// //
// class EmptyIterator //
// /////////////////// //

struct EmptyIterator : RealValuedOperator {

    //! Starting timestamp
    aku_Timestamp              begin_;
    //! Final timestamp
    aku_Timestamp              end_;

    EmptyIterator(aku_Timestamp begin, aku_Timestamp end)
        : begin_(begin)
        , end_(end)
    {
    }

    size_t get_size() const {
        return 0;
    }

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, double *destval, size_t size) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }

    virtual Direction get_direction() {
        if (begin_ < end_) {
            return Direction::FORWARD;
        }
        return Direction::BACKWARD;
    }
};

// //////////////////////// //
// class NBTreeSBlockFilter //
// //////////////////////// //


struct NBTreeSBlockFilter : NBTreeSBlockIteratorBase<double> {

    ValueFilter filter_;

    NBTreeSBlockFilter(std::shared_ptr<BlockStore> bstore,
                       LogicAddr addr,
                       aku_Timestamp begin,
                       aku_Timestamp end,
                       const ValueFilter& filter)
        : NBTreeSBlockIteratorBase<double>(bstore, addr, begin, end)
        , filter_(filter)
    {
    }

    NBTreeSBlockFilter(std::shared_ptr<BlockStore> bstore,
                       NBTreeSuperblock const& sblock,
                       aku_Timestamp begin,
                       aku_Timestamp end,
                       const ValueFilter& filter)
        : NBTreeSBlockIteratorBase<double>(bstore, sblock, begin, end)
        , filter_(filter)
    {
    }

    //! Create leaf iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_leaf_iterator(const SubtreeRef &ref) {
        assert(ref.type == NBTreeBlockType::LEAF);
        aku_Status status;
        std::shared_ptr<Block> block;
        std::tie(status, block) = read_and_check(bstore_, ref.addr);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, std::unique_ptr<RealValuedOperator>());
        }
        auto blockref = subtree_cast(block->get_cdata());
        assert(blockref->type == ref.type);
        std::unique_ptr<RealValuedOperator> result;
        switch (filter_.get_overlap(*blockref)) {
        case RangeOverlap::FULL_OVERLAP: {
            // Return normal leaf iterator because it's faster
            NBTreeLeaf leaf(block);
            result.reset(new NBTreeLeafIterator(begin_, end_, leaf));
            break;
        }
        case RangeOverlap::PARTIAL_OVERLAP: {
            // Return filtering leaf operator
            NBTreeLeaf leaf(block);
            result.reset(new NBTreeLeafFilter(begin_, end_, filter_, leaf));
            break;
        }
        case RangeOverlap::NO_OVERLAP: {
            // There is no data that can pass the filter so just return an empty iterator
            result.reset(new EmptyIterator(begin_, end_));
            break;
        }
        };
        return std::make_tuple(AKU_SUCCESS, std::move(result));
    }

    //! Create superblock iterator (used by `get_next_iter` template method).
    virtual std::tuple<aku_Status, TIter> make_superblock_iterator(const SubtreeRef &ref) {
        auto overlap = filter_.get_overlap(ref);
        TIter result;
        switch(overlap) {
        case RangeOverlap::FULL_OVERLAP:
            // Return normal superblock iterator
            result.reset(new NBTreeSBlockIterator(bstore_, ref.addr, begin_, end_));
            break;
        case RangeOverlap::PARTIAL_OVERLAP:
            // Return filter
            result.reset(new NBTreeSBlockFilter(bstore_, ref.addr, begin_, end_, filter_));
            break;
        case RangeOverlap::NO_OVERLAP:
            // Return dummy
            result.reset(new EmptyIterator(begin_, end_));
            break;
        }
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

class NBTreeLeafAggregator : public AggregateOperator {
    NBTreeLeafIterator iter_;
    bool enable_cached_metadata_;
    SubtreeRef metacache_;
public:
    template<class LeafT>
    NBTreeLeafAggregator(aku_Timestamp begin, aku_Timestamp end, LeafT const& node)
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

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destxs, size_t size);
    virtual Direction get_direction();
};

}
}
