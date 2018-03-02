#include "operator.h"

#include <cassert>

namespace Akumuli {
namespace StorageEngine {

void AggregationResult::copy_from(SubtreeRef const& r) {
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

void AggregationResult::do_the_math(aku_Timestamp* tss, double const* xss, size_t size, bool inverted) {
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

void AggregationResult::add(aku_Timestamp ts, double xs, bool forward) {
    sum += xs;
    if (min > xs) {
        min = xs;
        mints = ts;
    }
    if (max < xs) {
        max = xs;
        maxts = ts;
    }
    if (cnt == 0) {
        first = xs;
        if (forward) {
            _begin = ts;
        } else {
            _end = ts;
        }
    }
    last = xs;
    if (forward) {
        _end = ts;
    } else {
        _begin = ts;
    }
    cnt += 1;
}

void AggregationResult::combine(const AggregationResult& other) {
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

// ----------- //
// ValueFilter //
// ----------- //

ValueFilter::ValueFilter()
    : mask(0)
    , thresholds{0, 0, 0, 0}
{
}

bool ValueFilter::match(double value) const {
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

int ValueFilter::getRank() const {
    return __builtin_popcount(mask);
}

bool ValueFilter::isOrdered() const {
    if (getRank() == 2) {
        double hi = mask&(1 << LT) ? thresholds[LT]
                                   : thresholds[LE];
        double lo = mask&(1 << GT) ? thresholds[GT]
                                   : thresholds[GE];
        return lo < hi;
    }
    return true;
}

RangeOverlap ValueFilter::getOverlap(const SubtreeRef& ref) const {
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

ValueFilter& ValueFilter::less_than(double value) {
    mask          |= 1 << LT;
    thresholds[LT] = value;
    return *this;
}

ValueFilter& ValueFilter::less_or_equal(double value) {
    mask          |= 1 << LE;
    thresholds[LE] = value;
    return *this;
}

ValueFilter& ValueFilter::greater_than(double value) {
    mask          |= 1 << GT;
    thresholds[GT] = value;
    return *this;
}

ValueFilter& ValueFilter::greater_or_equal(double value) {
    mask          |= 1 << GE;
    thresholds[GE] = value;
    return *this;
}

bool ValueFilter::validate() const {
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

// --------------- //
// AggregateFilter //
// --------------- //

AggregateFilter::AggregateFilter()
    : bitmap(0)
{
}

bool AggregateFilter::setFilter(u32 op, const ValueFilter& filter) {
    if (op < N) {
        filters[op] = filter;
        bitmap |= (1 << op);
        return true;
    }
    return false;
}

bool AggregateFilter::match(const AggregationResult& res, AggregateFilter::Mode mode) const {
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

}}
