#pragma once

#include "operator.h"

// For MergeOperator
#include <boost/heap/skew_heap.hpp>
#include <boost/range.hpp>
#include <boost/range/iterator_range.hpp>

namespace Akumuli {
namespace StorageEngine {


/** Concatenating iterator.
  * Accepts list of iterators in the c-tor. All iterators then
  * can be seen as one iterator. Iterators should be in correct
  * order.
  */
struct ScanOperator : RealValuedOperator {
    typedef std::vector<std::unique_ptr<RealValuedOperator>> IterVec;
    IterVec   iter_;
    Direction dir_;
    u32       iter_index_;

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    ScanOperator(TVec&& iter)
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


/**
 * Materializes list of series by chaining them
 */
class ChainOperator : public TupleOperator {
    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
public:
    ChainOperator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it);
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};

template<int dir>  // 0 - forward, 1 - backward
struct TimeOrder {
    typedef std::tuple<aku_Timestamp, aku_ParamId> KeyType;
    typedef std::tuple<KeyType, double, u32> HeapItem;
    std::greater<KeyType> greater_;
    std::less<KeyType> less_;

    bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
        if (dir == 0) {
            return greater_(std::get<0>(lhs), std::get<0>(rhs));
        }
        return less_(std::get<0>(lhs), std::get<0>(rhs));
    }
};
}}  // namespace
