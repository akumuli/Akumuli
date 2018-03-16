#pragma once
#include "akumuli_def.h"
#include "operators/operator.h"

#include <tuple>
#include <vector>
#include <cassert>

namespace Akumuli {

struct TupleOutputUtils {
    /** Get pointer to buffer and return pointer to sample and tuple data */
    static std::tuple<aku_Sample*, double*> cast(u8* dest) {
        aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
        double* tuple      = reinterpret_cast<double*>(sample->payload.data);
        return std::make_tuple(sample, tuple);
    }

    static double get_flags(std::vector<StorageEngine::AggregationFunction> const& tup) {
        // Shift will produce power of two (e.g. if tup.size() == 3 then
        // (1 << tup.size) will give us 8, 8-1 is 7 (exactly three lower
        // bits is set)).
        union {
            double d;
            u64 u;
        } bits;
        bits.u = (1ull << tup.size()) - 1;
        // Save number of elements in the bitflags
        bits.u |= tup.size() << 58;
        return bits.d;
    }

    // Returns size of the tuple and bitmap
    static std::tuple<u32, u64> get_size_and_bitmap(double value) {
        union {
            double d;
            u64 u;
        } bits;
        bits.d = value;
        u32 size = static_cast<u32>(bits.u >> 58);
        u32 bitmap = 0x3ffffffffffffff & bits.u;
        return std::make_tuple(size, bitmap);
    }

    static double get(StorageEngine::AggregationResult const& res, StorageEngine::AggregationFunction afunc) {
        double out = 0;
        switch (afunc) {
        case StorageEngine::AggregationFunction::CNT:
            out = res.cnt;
            break;
        case StorageEngine::AggregationFunction::SUM:
            out = res.sum;
            break;
        case StorageEngine::AggregationFunction::MIN:
            out = res.min;
            break;
        case StorageEngine::AggregationFunction::MIN_TIMESTAMP:
            out = static_cast<double>(res.mints);
            break;
        case StorageEngine::AggregationFunction::MAX:
            out = res.max;
            break;
        case StorageEngine::AggregationFunction::MAX_TIMESTAMP:
            out = res.maxts;
            break;
        case StorageEngine::AggregationFunction::MEAN:
            out = res.sum / res.cnt;
            break;
        }
        return out;
    }

    static void set_tuple(double* tuple, std::vector<StorageEngine::AggregationFunction> const& comp, StorageEngine::AggregationResult const& res) {
        for (size_t i = 0; i < comp.size(); i++) {
            auto elem = comp[i];
            *tuple = get(res, elem);
            tuple++;
        }
    }

    static size_t get_tuple_size(const std::vector<StorageEngine::AggregationFunction>& tup) {
        size_t payload = 0;
        assert(!tup.empty());
        payload = sizeof(double)*tup.size();
        return sizeof(aku_Sample) + payload;
    }

    static double get_first_value(const aku_Sample* sample) {
        union {
            double d;
            u64 u;
        } bits;
        bits.d = sample->payload.float64;
        AKU_UNUSED(bits);
        assert(sample->payload.type == AKU_PAYLOAD_TUPLE && bits.u == 0x400000000000001ul);
        const double* value = reinterpret_cast<const double*>(sample->payload.data);
        return *value;
    }

    static void set_first_value(aku_Sample* sample, double x) {
        union {
            double d;
            u64 u;
        } bits;
        bits.d = sample->payload.float64;
        AKU_UNUSED(bits);
        assert(sample->payload.type == AKU_PAYLOAD_TUPLE && bits.u == 0x400000000000001ul);
        double* value = reinterpret_cast<double*>(sample->payload.data);
        *value = x;
    }

    static aku_Sample* copy_sample(const aku_Sample* src, char* dest, size_t dest_size) {
        size_t sample_size = std::max(static_cast<size_t>(src->payload.size), sizeof(aku_Sample));
        if (sample_size > dest_size) {
            return nullptr;
        }
        memcpy(dest, src, sample_size);
        return reinterpret_cast<aku_Sample*>(dest);
    }

    static bool is_one_element_tuple(const aku_Sample* sample) {
        if (sample->payload.type == AKU_PAYLOAD_TUPLE) {
            union {
                double d;
                u64 u;
            } bits;
            bits.d = sample->payload.float64;
            if (bits.u != 0x400000000000001ul) {
                // only one element tuples supported
                return false;
            }
            return true;
        }
        return false;
    }
};

}
