#pragma once

/**
  * @file operator.h contains operator interfaces.
  * Operator performs some processing on data. Operator can work on series level. In this case
  * it doesn't know anything about other series (or columns). Good example of such operator is
  * an aggregate operator that computes some aggregate function on data. Operator can work on
  * tuple level. Tuples are produced from individual series through materialization procedure.
  * Example of such operator is a join operator. This operator consumes several series operators
  * and produces sequence of tuples.
  */

#include "akumuli_def.h"
#include "../nbtree_def.h"


namespace Akumuli {
namespace StorageEngine {

enum class AggregationFunction {
    MIN,
    MAX,
    SUM,
    CNT,
    MIN_TIMESTAMP,
    MAX_TIMESTAMP,
    MEAN
};

//! Result of the aggregation operation that has several components.
struct AggregationResult {
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
    void combine(const AggregationResult& other);
};


static const AggregationResult INIT_AGGRES = {
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


/** Single series operator.
  * @note all ranges is semi-open. This means that if we're
  *       reading data from A to B, operator should return
  *       data in range [A, B), and B timestamp should be
  *       greater (or less if we're reading data in backward
  *       direction) then all timestamps that we've read before.
  */
template <class TValue>
struct SeriesOperator {

    //! Iteration direction
    enum class Direction {
        FORWARD, BACKWARD,
    };

    //! D-tor
    virtual ~SeriesOperator() = default;

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
using RealValuedOperator = SeriesOperator<double>;

//! Base class for all aggregating iterators. Return single value.
using AggregateOperator = SeriesOperator<AggregationResult>;


/** This interface is used by column-store internally.
  * It materializes tuples/values and produces a series of aku_Sample values.
  */
struct ColumnMaterializer {

    virtual ~ColumnMaterializer() = default;

    /** Read samples in batch.
      * Samples can be of variable size.
      * @param dest is a pointer to buffer that will receive series of aku_Sample values
      * @param size is a size of the buffer in bytes
      * @return status of the operation (success or error code) and number of written bytes
      */
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) = 0;
};

enum class RangeOverlap {
    NO_OVERLAP,
    FULL_OVERLAP,
    PARTIAL_OVERLAP
};

struct ValueFilter {
    enum {
        LT = 0,  //! Less than
        LE = 1,  //! Less or equal
        GT = 2,  //! Greater than
        GE = 3,  //! Greater or equal
        MAX_INDEX = 4,
    };

    //! Encode threshold mask
    int    mask;
    //! Thresholds
    double thresholds[4];

    ValueFilter()
        : mask(0)
        , thresholds{0, 0, 0, 0}
    {
    }

    bool match(double value) const {
        bool result = true;
        if (mask & (1 << LT)) {
            result &= value <  thresholds[LT];
        }
        else if (mask & (1 << LE)) {
            result &= value <= thresholds[LE];
        }
        if (mask & (1 << GT)) {
            result &= value >  thresholds[GT];
        }
        else if (mask & (1 << GE)) {
            result &= value >= thresholds[GE];
        }
        return result;
    }

    /**
     * @brief Get rank
     * Filter rank is 0 if it's empty.
     * Filter rank is 1 if only one bound is set (lower or upper).
     * Filter rank is 2 if both bounds are set.
     * @return filter rank
     */
    int getRank() const {
        return __builtin_popcount(mask);
    }

    /**
     * Return true if the filter is ordered (lowerbound is less than upperbound).
     * Onesided filter is always ordered.
     */
    bool isOrdered() const {
        if (getRank() == 2) {
            double hi = mask&(1 << LT) ? thresholds[LT]
                                       : thresholds[LE];
            double lo = mask&(1 << GT) ? thresholds[GT]
                                       : thresholds[GE];
            return lo < hi;
        }
        return true;
    }

    RangeOverlap getOverlap(const SubtreeRef& ref) const {
        if (getRank() < 2) {
            bool begin = match(ref.min);
            bool end   = match(ref.max);
            if (begin && end) {
                return RangeOverlap::FULL_OVERLAP;
            }
            else if (begin || end) {
                return RangeOverlap::PARTIAL_OVERLAP;
            } else {
                return RangeOverlap::NO_OVERLAP;
            }
        } else {
            // Rank is two, use range overlap algorithm
            double hi = mask&(1 << LT) ? thresholds[LT]
                                       : thresholds[LE];
            double lo = mask&(1 << GT) ? thresholds[GT]
                                       : thresholds[GE];
            double min = std::min(ref.min, lo);
            double max = std::max(ref.max, hi);
            double w1  = ref.max - ref.min;
            double w2  = hi - lo;
            bool inclusive = (mask&(1 << LE)) && (mask&(1 << GE));
            bool overlap = inclusive ? max - min <= w1 + w2
                                     : max - min <  w1 + w2;
            if (overlap) {
                // Overlap
                bool begin = match(ref.min);
                bool end   = match(ref.max);
                if (begin && end) {
                    return RangeOverlap::FULL_OVERLAP;
                }
                return RangeOverlap::PARTIAL_OVERLAP;
            }
            return RangeOverlap::NO_OVERLAP;
        }
    }

    ValueFilter& less_than(double value) {
        mask          |= 1 << LT;
        thresholds[LT] = value;
        return *this;
    }

    ValueFilter& less_or_equal(double value) {
        mask          |= 1 << LE;
        thresholds[LE] = value;
        return *this;
    }

    ValueFilter& greater_than(double value) {
        mask          |= 1 << GT;
        thresholds[GT] = value;
        return *this;
    }

    ValueFilter& greater_or_equal(double value) {
        mask          |= 1 << GE;
        thresholds[GE] = value;
        return *this;
    }

    //! Check filter invariant
    bool validate() const {
        if (mask == 0) {
            return false;
        }
        if ((mask & (1 << LT)) && (mask & (1 << LE))) {
            return false;
        }
        if ((mask & (1 << GT)) && (mask & (1 << GE))) {
            return false;
        }
        return isOrdered();
    }
};

struct AggregateFilter {
    /*
    double cnt;
    double sum;
    double min;
    double max;
    double first;
    double last;
    */
    enum {
        AVG,
        MIN,
        MAX,
        FIRST,
        LAST,
        N = 5,
    };

    enum class Mode {
        ALL,
        ANY
    };

    ValueFilter filters[N];
    u32 bitmap;

    AggregateFilter()
        : bitmap(0)
    {
    }

    /** Set filter, return true on success.
     * @param op is an aggregate name (AVG, MIN...)
     * @param filter is a filter that shuld be used for such aggregate
     * @return true if set, false otherwise
     */
    bool setFilter(u32 op, const ValueFilter& filter) {
        if (op < N) {
            filters[op] = filter;
            bitmap |= (1 << op);
            return true;
        }
        return false;
    }

    bool match(const AggregationResult& res, Mode mode) const {
        bool result = mode == Mode::ALL;
        for (u32 bit = 0; bit < N; bit++) {
            u32 mask = 1 << bit;
            if (bitmap & mask) {
                const auto& flt = filters[bit];
                double value = 0.;
                if (bit == AVG) {
                    value = res.sum / res.cnt;
                }
                else if (bit == MIN) {
                    value = res.min;
                }
                else if (bit == MAX) {
                    value = res.max;
                }
                else if (bit == FIRST) {
                    value = res.first;
                }
                else if (bit == LAST) {
                    value = res.last;
                }
                if (mode == Mode::ALL) {
                    result &= flt.match(value);
                } else {
                    result |= flt.match(value);
                }
            }
        }
        return result;
    }
};

}
}
