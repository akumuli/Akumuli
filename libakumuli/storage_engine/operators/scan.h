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
struct ChainOperator : RealValuedOperator {
    typedef std::vector<std::unique_ptr<RealValuedOperator>> IterVec;
    IterVec   iter_;
    Direction dir_;
    u32       iter_index_;

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    ChainOperator(TVec&& iter)
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
 * Materializes list of columns by chaining them
 */
class ChainMaterializer : public ColumnMaterializer {
    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
public:
    ChainMaterializer(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it);
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};

}}  // namespace
