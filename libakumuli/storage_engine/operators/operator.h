#pragma once

#include "akumuli_def.h"
#include "../nbtree_def.h"

namespace Akumuli {
namespace StorageEngine {

//! Result of the aggregation operation that has several components.
struct NBTreeAggregationResult {
    double cnt;
    double sum;
    double min;
    double max;
    double first;
    double last;
    aku_Timestamp mints;
    aku_Timestamp maxts;
    aku_Timestamp _begin;
    aku_Timestamp _end;

    //! Copy all components from subtree reference.
    void copy_from(SubtreeRef const&);
    //! Calculate values from raw data.
    void do_the_math(aku_Timestamp *tss, double const* xss, size_t size, bool inverted);
    /**
     * Add value to aggregate
     * @param ts is a timestamp
     * @param xs is a value
     * @param forward is used to indicate external order of added elements
     */
    void add(aku_Timestamp ts, double xs, bool forward);
    //! Combine this value with the other one (inplace update).
    void combine(const NBTreeAggregationResult& other);
};


static const NBTreeAggregationResult INIT_AGGRES = {
    .0,
    .0,
    std::numeric_limits<double>::max(),
    std::numeric_limits<double>::lowest(),
    .0,
    .0,
    std::numeric_limits<aku_Timestamp>::max(),
    std::numeric_limits<aku_Timestamp>::lowest(),
    std::numeric_limits<aku_Timestamp>::max(),
    std::numeric_limits<aku_Timestamp>::lowest(),
};


/** Database query operator.
  * @note all ranges is semi-open. This means that if we're
  *       reading data from A to B, operator should return
  *       data in range [A, B), and B timestamp should be
  *       greater (or less if we're reading data in backward
  *       direction) then all timestamps that we've read before.
  */
template <class TValue>
struct QueryOperator {

    //! Iteration direction
    enum class Direction {
        FORWARD, BACKWARD,
    };

    //! D-tor
    virtual ~QueryOperator() = default;

    /** Read next portion of data.
      * @param destts Timestamps destination buffer. On success timestamps will be written here.
      * @param destval Values destination buffer.
      * @param size Size of the  destts and destval buffers (should be the same).
      * @return status and number of elements written to both buffers.
      */
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp* destts, TValue* destval, size_t size) = 0;

    virtual Direction get_direction() = 0;
};

//! Base class for all raw data iterators.
using RealValuedOperator = QueryOperator<double>;

//! Base class for all aggregating iterators. Return single value.
using AggregateOperator = QueryOperator<NBTreeAggregationResult>;

}
}
