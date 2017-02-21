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

}}
