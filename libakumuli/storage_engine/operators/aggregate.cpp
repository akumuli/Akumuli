#include "aggregate.h"

#include <cassert>

namespace Akumuli {
namespace StorageEngine {


std::tuple<aku_Status, size_t> CombineAggregateOperator::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
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
    int nagg = 0;
    while(iter_index_ < iter_.size()) {
        std::tie(status, ressz) = iter_[iter_index_]->read(outts.data(), outval.data(), SZBUF);
        if (ressz != 0) {
            xsresult = std::accumulate(outval.begin(), outval.begin() + ressz, xsresult,
                            [](NBTreeAggregationResult lhs, NBTreeAggregationResult rhs) {
                                lhs.combine(rhs);
                                return lhs;
                            });
            tsresult = outts[static_cast<size_t>(ressz)-1];
            nagg++;
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
    size_t result_size = 0;
    if (nagg != 0) {
        result_size = 1;
        destts[0] = tsresult;
        destval[0] = xsresult;
    }
    return std::make_tuple(AKU_SUCCESS, result_size);
}

AggregateOperator::Direction CombineAggregateOperator::get_direction() {
    return dir_;
}


// Group aggregate operator //


bool CombineGroupAggregateOperator::can_read() const {
    return rdpos_ < rdbuf_.size();
}


u32 CombineGroupAggregateOperator::elements_in_rdbuf() const {
    return static_cast<u32>(rdbuf_.size()) - rdpos_;  // Safe to cast because rdbuf_.size() <= RDBUF_SIZE
}


std::tuple<aku_Status, size_t> CombineGroupAggregateOperator::copy_to(aku_Timestamp* desttx, NBTreeAggregationResult* destxs, size_t size) {
    aku_Status status = AKU_SUCCESS;
    size_t copied = 0;
    while (status == AKU_SUCCESS && size > 0) {
        size_t n = elements_in_rdbuf();
        if (iter_index_ != iter_.size()) {
            if (n < 2) {
                status = refill_read_buffer();
                continue;
            }
            // We can copy last element of the rdbuf_ to the output only if all
            // iterators were consumed! Otherwise invariant will be broken.
            n--;
        } else {
            if (n == 0) {
                break;
            }
        }

        auto tocopy = std::min(n, size);

        // Copy elements
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


aku_Status CombineGroupAggregateOperator::refill_read_buffer() {
    aku_Status status = AKU_ENO_DATA;
    if (iter_index_ == iter_.size()) {
        return AKU_ENO_DATA;
    }

    u32 pos = 0;
    if (!rdbuf_.empty()) {
        auto tail = rdbuf_.back();  // the last element should be saved because it is possible that
                                    // it's not full (part of the range contained in first iterator
                                    // and another part in second iterator or even in more than one
                                    // iterators).
        rdbuf_.clear();
        rdbuf_.resize(RDBUF_SIZE, INIT_AGGRES);
        rdpos_ = 0;
        rdbuf_.at(0) = tail;
        pos = 1;
    } else {
        rdbuf_.clear();
        rdbuf_.resize(RDBUF_SIZE, INIT_AGGRES);
        rdpos_ = 0;
    }

    while(iter_index_ < iter_.size()) {
        size_t size = rdbuf_.size() - pos;
        if (size == 0) {
            break;
        }
        // NOTE: We can't read data from iterator directly to the rdbuf_ because buckets
        //       can be split between two iterators. In this case we should join such
        //       values together.
        // Invariant: rdbuf_ have enough room to fit outxs in the worst case (the worst
        //       case: `read` returns `size` elements and ranges isn't overlapping and we
        //       don't need to merge last element of `rdbuf_` with first elem of `outxs`).
        std::vector<NBTreeAggregationResult> outxs(size, INIT_AGGRES);
        std::vector<aku_Timestamp>           outts(size, 0);
        u32 outsz;
        std::tie(status, outsz) = iter_[iter_index_]->read(outts.data(), outxs.data(), outxs.size());
        if (outsz != 0) {
            if (pos > 0) {
                // Check last and first values of rdbuf_ and outxs
                auto const& last  = rdbuf_.at(pos - 1);
                auto const& first = outxs.front();
                auto const  delta = dir_ == Direction::FORWARD ? first._begin - last._begin
                                                               : last._end - first._end;
                if (delta < step_) {
                    pos--;
                }
            }
        }
        for (size_t ix = 0; ix < outsz; ix++) {
            rdbuf_.at(pos).combine(outxs.at(ix));
            const auto newdelta = rdbuf_.at(pos)._end - rdbuf_.at(pos)._begin;
            if (newdelta > step_) {
                assert(newdelta <= step_);
            }
            pos++;
        }
        if (status == AKU_ENO_DATA) {
            // This leaf node is empty, continue with next.
            iter_index_++;
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Failure, stop iteration.
            rdbuf_.resize(pos);
            return status;
        }
    }
    rdbuf_.resize(pos);
    return AKU_SUCCESS;
}

std::tuple<aku_Status, size_t> CombineGroupAggregateOperator::read(aku_Timestamp *destts, NBTreeAggregationResult *destval, size_t size) {
    if (size == 0) {
        return std::make_tuple(AKU_EBAD_ARG, 0);
    }
    return copy_to(destts, destval, size);
}

CombineGroupAggregateOperator::Direction CombineGroupAggregateOperator::get_direction() {
    return dir_;
}


}}
