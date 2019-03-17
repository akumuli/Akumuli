#include "nbtree_iter.h"

#include <sstream>

namespace Akumuli {
namespace StorageEngine {

std::tuple<aku_Status, std::shared_ptr<Block>> read_and_check(std::shared_ptr<BlockStore> bstore, LogicAddr curr) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = bstore->read_block(curr);
    if (status != AKU_SUCCESS) {
        return std::tie(status, block);
    }
    // Check consistency (works with both inner and leaf nodes).
    u8 const* data = block->get_cdata();
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

RealValuedOperator::Direction NBTreeLeafIterator::get_direction() {
    if (begin_ < end_) {
        return Direction::FORWARD;
    }
    return Direction::BACKWARD;
}



// //////////////////////////// //
// NBTreeSBlockCandlesticksIter //
// //////////////////////////// //

std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> NBTreeSBlockCandlesticsIter::make_leaf_iterator(const SubtreeRef &ref) {
    auto agg = INIT_AGGRES;
    agg.copy_from(ref);
    std::unique_ptr<AggregateOperator> result;
    result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> NBTreeSBlockCandlesticsIter::make_superblock_iterator(const SubtreeRef &ref) {
    aku_Timestamp min = std::min(begin_, end_);
    aku_Timestamp max = std::max(begin_, end_);
    aku_Timestamp delta = max - min;
    std::unique_ptr<AggregateOperator> result;
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

std::tuple<aku_Status, size_t> NBTreeSBlockCandlesticsIter::read(aku_Timestamp *destts, AggregationResult *destval, size_t size) {
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

std::tuple<aku_Status, size_t> ValueAggregator::read(aku_Timestamp *destts, AggregationResult *destval, size_t size) {
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

std::tuple<aku_Status, size_t> NBTreeSBlockAggregator::read(aku_Timestamp *destts, AggregationResult *destval, size_t size) {
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
    std::vector<AggregationResult> xss(SZBUF, INIT_AGGRES);
    std::vector<aku_Timestamp> tss(SZBUF, 0);
    aku_Timestamp outts = 0;
    AggregationResult outxs = INIT_AGGRES;
    ssize_t outsz = 0;
    aku_Status status;
    int nagg = 0;
    while(true) {
        std::tie(status, outsz) = iter(tss.data(), xss.data(), SZBUF);
        if ((status == AKU_SUCCESS || status == AKU_ENO_DATA) && outsz != 0) {
            outts = tss[static_cast<size_t>(outsz)];
            outxs = std::accumulate(xss.begin(), xss.begin() + outsz, outxs,
                        [&](AggregationResult lhs, AggregationResult rhs) {
                            lhs.combine(rhs);
                            return lhs;
                        });
            size = 1;
            nagg++;
        } else if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
            size = 0;
            break;
        } else if (outsz == 0) {
            if (nagg) {
                destval[0] = outxs;
                destts[0] = outts;
                size = 1;
            } else {
                size = 0;
            }
            break;
        }
    }
    return std::make_tuple(status, size);
}

std::tuple<aku_Status, std::unique_ptr<AggregateOperator> > NBTreeSBlockAggregator::make_leaf_iterator(SubtreeRef const& ref) {
    if (!bstore_->exists(ref.addr)) {
        TIter empty;
        return std::make_tuple(AKU_EUNAVAILABLE, std::move(empty));
    }
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = read_and_check(bstore_, ref.addr);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<AggregateOperator>());
    }
    leftmost_leaf_found_ = true;
    NBTreeLeaf leaf(block);
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeLeafAggregator(begin_, end_, leaf));
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, std::unique_ptr<AggregateOperator> > NBTreeSBlockAggregator::make_superblock_iterator(SubtreeRef const& ref) {
    if (!bstore_->exists(ref.addr)) {
        TIter empty;
        return std::make_tuple(AKU_EUNAVAILABLE, std::move(empty));
    }
    aku_Timestamp min = std::min(begin_, end_);
    aku_Timestamp max = std::max(begin_, end_);
    std::unique_ptr<AggregateOperator> result;
    if (leftmost_leaf_found_ && (min <= ref.begin && ref.end < max)) {
        // We don't need to go to lower level, value from subtree ref can be used instead.
        auto agg = INIT_AGGRES;
        agg.copy_from(ref);
        result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    } else {
        result.reset(new NBTreeSBlockAggregator(bstore_, ref.addr, begin_, end_));
    }
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}


NBTreeLeafGroupAggregator::Direction NBTreeLeafGroupAggregator::get_direction() {
    return iter_.get_direction() == NBTreeLeafIterator::Direction::FORWARD ? Direction::FORWARD : Direction::BACKWARD;
}

std::tuple<aku_Status, size_t> NBTreeLeafGroupAggregator::read(aku_Timestamp *destts, AggregationResult *destxs, size_t size) {
    size_t outix = 0;
    if (size == 0) {
        return std::make_tuple(AKU_EBAD_ARG, 0);
    }
    if (enable_cached_metadata_) {
        if (metacache_.count == 0) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        // Fast path. Use metadata to compute results.
        destts[0] = metacache_.begin;
        destxs[0].copy_from(metacache_);
        auto delta = destxs[0]._end - destxs[0]._begin;
        if (delta > step_) {
            assert(delta <= step_);
        }
        enable_cached_metadata_ = false;  // next call to `read` should return AKU_ENO_DATA
        return std::make_tuple(AKU_SUCCESS, 1);
    } else {
        if (!iter_.get_size()) {
            // Second call to read will lead here if fast path have been taken on first call.
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        size_t size_hint = std::min(iter_.get_size(), size);
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
        int valcnt = 0;
        AggregationResult outval = INIT_AGGRES;
        const bool forward = begin_ < end_;
        u64 bin = 0;
        for (size_t ix = 0; ix < out_size; ix++) {
            aku_Timestamp normts = forward ? ts[ix] - begin_
                                           : begin_ - ts[ix];
            if (valcnt == 0) {
                bin = normts / step_;
            } else if (normts / step_ != bin) {
                bin = normts / step_;
                destxs[outix] = outval;
                destts[outix] = outval._begin;
                outix++;
                outval = INIT_AGGRES;
            }
            valcnt++;
            outval.add(ts[ix], xs[ix], forward);
            // Check invariant
            auto delta = outval._end - outval._begin;
            if (delta > step_) {
                assert(delta <= step_);
            }
        }
        if (outval.cnt > 0) {
            destxs[outix] = outval;
            destts[outix] = outval._begin;
            outix++;
        }
    }
    assert(outix <= size);
    return std::make_tuple(AKU_SUCCESS, outix);
}


std::tuple<aku_Status, size_t> NBTreeSBlockGroupAggregator::read(aku_Timestamp *destts,
                                                                 AggregationResult *destval,
                                                                 size_t size)
{
    if (size == 0) {
        return std::make_pair(AKU_EBAD_ARG, 0ul);
    }
    if (!fsm_pos_ ) {
        aku_Status status = init();
        if (status != AKU_SUCCESS) {
            return std::make_pair(status, 0ul);
        }
        fsm_pos_++;
    }
    return copy_to(destts, destval, size);
}

std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> NBTreeSBlockGroupAggregator::make_leaf_iterator(SubtreeRef const& ref) {
    aku_Status status;
    std::shared_ptr<Block> block;
    std::tie(status, block) = read_and_check(bstore_, ref.addr);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<AggregateOperator>());
    }
    NBTreeLeaf leaf(block);
    std::unique_ptr<AggregateOperator> result;
    result.reset(new NBTreeLeafGroupAggregator(begin_, end_, step_, leaf));
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, std::unique_ptr<AggregateOperator>> NBTreeSBlockGroupAggregator::make_superblock_iterator(SubtreeRef const& ref) {
    std::unique_ptr<AggregateOperator> result;
    bool inner = false;
    if (get_direction() == Direction::FORWARD) {
        auto const query_boundary = (end_ - begin_) / step_;
        auto const start_bucket = (ref.begin - begin_) / step_;
        auto const stop_bucket = (ref.end - begin_) / step_;
        if (start_bucket == stop_bucket && stop_bucket != query_boundary) {
            inner = true;
        }
    } else {
        auto const query_boundary = (begin_ - end_) / step_;
        auto const start_bucket = (begin_ - ref.end) / step_;
        auto const stop_bucket = (begin_ - ref.begin) / step_;
        if (start_bucket == stop_bucket && stop_bucket != query_boundary) {
            inner = true;
        }
    }
    if (inner) {
        // We don't need to go to lower level, value from subtree ref can be used instead.
        auto agg = INIT_AGGRES;
        agg.copy_from(ref);
        result.reset(new ValueAggregator(ref.end, agg, get_direction()));
    } else {
        result.reset(new NBTreeSBlockGroupAggregator(bstore_, ref.addr, begin_, end_, step_));
    }
    return std::make_tuple(AKU_SUCCESS, std::move(result));
}

std::tuple<aku_Status, size_t> NBTreeLeafFilter::read(aku_Timestamp *destts, double *destval, size_t size) {
    ssize_t sz = static_cast<ssize_t>(size);
    if (status_ != AKU_SUCCESS) {
        return std::make_tuple(status_, 0);
    }
    ssize_t toread = tsbuf_.size() - pos_;
    if (toread > sz) {
        toread = sz;
    }
    if (toread == 0) {
        return std::make_tuple(AKU_ENO_DATA, 0);
    }
    auto begin = pos_;
    ssize_t end = pos_ + toread;
    std::copy(tsbuf_.begin() + begin, tsbuf_.begin() + end, destts);
    std::copy(xsbuf_.begin() + begin, xsbuf_.begin() + end, destval);
    pos_ += toread;
    return std::make_tuple(AKU_SUCCESS, toread);
}

RealValuedOperator::Direction NBTreeLeafFilter::get_direction() {
    if (begin_ < end_) {
        return Direction::FORWARD;
    }
    return Direction::BACKWARD;
}

NBTreeLeafAggregator::Direction NBTreeLeafAggregator::get_direction() {
    return iter_.get_direction() == NBTreeLeafIterator::Direction::FORWARD ? Direction::FORWARD : Direction::BACKWARD;
}

std::tuple<aku_Status, size_t> NBTreeLeafAggregator::read(aku_Timestamp *destts, AggregationResult *destxs, size_t size) {
    aku_Timestamp outts = 0;
    AggregationResult outval = INIT_AGGRES;
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


}
}
