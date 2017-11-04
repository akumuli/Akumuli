#pragma once

#include <cassert>

#include "operator.h"
#include "merge.h"
#include "../tuples.h"

namespace Akumuli {
namespace StorageEngine {


/** Aggregating operator.
  * Accepts list of iterators in the c-tor. All iterators then
  * can be seen as one iterator that returns single value.
  */
struct CombineAggregateOperator : AggregateOperator {
    typedef std::vector<std::unique_ptr<AggregateOperator>> IterVec;
    IterVec             iter_;
    Direction           dir_;
    u32                 iter_index_;

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    CombineAggregateOperator(TVec&& iter)
        : iter_(std::forward<TVec>(iter))
        , iter_index_(0)
    {
        if (iter_.empty()) {
            dir_ = Direction::FORWARD;
        } else {
            dir_ = iter_.front()->get_direction();
        }
    }

    void add(std::unique_ptr<AggregateOperator>&& it);

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size);
    virtual Direction get_direction();
};


/** Aggregating operator (group-by + aggregate).
  */
struct CombineGroupAggregateOperator : AggregateOperator {
    typedef std::vector<std::unique_ptr<AggregateOperator>> IterVec;
    typedef std::vector<AggregationResult> ReadBuffer;
    const aku_Timestamp begin_;
    const u64           step_;
    IterVec             iter_;
    Direction           dir_;
    u32                 iter_index_;
    ReadBuffer          rdbuf_;
    u32                 rdpos_;

    // NOTE: object of this class joins several iterators into one. Time intervals
    // covered by this iterators shouldn't overlap. Each iterator should be group-
    // aggregate iterator. This iterators output contains aggregated values. Each
    // value covers time interval defined by `step_` variable. The first and the last
    // values returned by each iterator can be incomplete (contain only part of the
    // range). In this case `GroupAggregate` iterator should join the last value of
    // the previous iterator with the first one of the next iterator.
    //
    //

    enum {
        RDBUF_SIZE = 0x100,
    };

    //! C-tor. Create iterator from list of iterators.
    template<class TVec>
    CombineGroupAggregateOperator(aku_Timestamp begin, u64 step, TVec&& iter)
        : begin_(begin)
        , step_(step)
        , iter_(std::forward<TVec>(iter))
        , iter_index_(0)
        , rdpos_(0)
    {
        if (iter_.empty()) {
            dir_ = Direction::FORWARD;
        } else {
            dir_ = iter_.front()->get_direction();
        }
    }

    //! Return true if `rdbuf_` is not empty and have some data to read.
    bool can_read() const;

    //! Return number of elements in rdbuf_ available for reading
    u32 elements_in_rdbuf() const;

    /**
     * @brief Copy as much elements as possible to the dest arrays.
     * @param desttx timestamps array
     * @param destxs values array
     * @param size size of both arrays
     * @return number of elements copied
     */
    std::tuple<aku_Status, size_t> copy_to(aku_Timestamp* desttx, AggregationResult* destxs, size_t size);

    /**
     * @brief Refils read buffer.
     * @return AKU_SUCCESS on success, AKU_ENO_DATA if there is no more data to read, error code on error
     */
    aku_Status refill_read_buffer();

    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp *destts, AggregationResult *destval, size_t size);
    virtual Direction get_direction();
};


/**
 * Performs materialization for aggregate queries
 */
class AggregateMaterializer : public ColumnMaterializer {
    std::vector<std::unique_ptr<AggregateOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
    AggregationFunction func_;
public:
    AggregateMaterializer(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<AggregateOperator>>&& it, AggregationFunction func);

    /**
     * @brief read data from iterators collection
     * @param dest is a destination for aggregate
     * @param size size of both array
     * @return status and number of elements in dest
     */
    std::tuple<aku_Status, size_t> read(u8* dest, size_t size);
};

struct SeriesOrderAggregateMaterializer : TupleOutputUtils, ColumnMaterializer {
    std::vector<std::unique_ptr<AggregateOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    std::vector<AggregationFunction> tuple_;
    u32 pos_;

    SeriesOrderAggregateMaterializer(std::vector<aku_ParamId>&& ids,
                        std::vector<std::unique_ptr<AggregateOperator>>&& it,
                        const std::vector<AggregationFunction>& components)
        : iters_(std::move(it))
        , ids_(std::move(ids))
        , tuple_(std::move(components))
        , pos_(0)
    {
    }

    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) override;
};


struct TimeOrderAggregateMaterializer : TupleOutputUtils, ColumnMaterializer {
    typedef MergeJoinMaterializer<MergeJoinUtil::OrderByTimestamp> Materializer;
    std::unique_ptr<Materializer> join_iter_;

    TimeOrderAggregateMaterializer(const std::vector<aku_ParamId>& ids,
                      std::vector<std::unique_ptr<AggregateOperator>> &it,
                      const std::vector<AggregationFunction>& components)
    {
        assert(it.size());
        bool forward = it.front()->get_direction() == AggregateOperator::Direction::FORWARD;
        std::vector<std::unique_ptr<ColumnMaterializer>> iters;
        for (size_t i = 0; i < ids.size(); i++) {
            std::unique_ptr<ColumnMaterializer> iter;
            auto agg = std::move(it.at(i));
            std::vector<std::unique_ptr<AggregateOperator>> agglist;
            agglist.push_back(std::move(agg));
            auto ptr = new SeriesOrderAggregateMaterializer({ ids[i] }, std::move(agglist), components);
            iter.reset(ptr);
            iters.push_back(std::move(iter));
        }
        join_iter_.reset(new Materializer(std::move(iters), forward));
    }

    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) override {
        return join_iter_->read(dest, size);
    }
};

}}
