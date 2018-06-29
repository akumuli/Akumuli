#pragma once

#include "nbtree_def.h"
#include "compression.h"
#include <algorithm>

namespace Akumuli {
namespace StorageEngine {

//! Naive implementation of the ref-storage
template<class TreeT>
struct ConsolidatedRefStorage {

    std::vector<SubtreeRef> refs_;

    void append(const SubtreeRef& ref) {
        if (refs_.capacity() == refs_.size()) {
            // Grow buffer in small steps to prevent fragmentation
            const size_t grow_step = 1;
            refs_.reserve(refs_.capacity() + grow_step);
        }
        refs_.push_back(ref);
    }

    //! Return true if buffer can accomodate the value
    bool has_space(u16 level) const {
        return nelements(level) < AKU_NBTREE_FANOUT;
    }

    aku_Status loadFrom(const TreeT& sblock) {
        if (sblock.nelements() > refs_.capacity() - refs_.size()) {
            refs_.reserve(refs_.capacity() + sblock.nelements());
        }
        return sblock.read_all(&refs_);
    }

    aku_Status saveTo(TreeT* sblock) {
        aku_Status status = AKU_SUCCESS;
        for (const SubtreeRef& ref: refs_) {
            if (ref.level == sblock->get_level() - 1) {
                status = sblock->append(ref);
                if (status != AKU_SUCCESS) {
                    break;
                }
            }
        }
        return status;
    }

    int nelements(u16 level) const {
        auto res = std::count_if(refs_.begin(), refs_.end(), [level](const SubtreeRef& cur) {
            return cur.level == level;
        });
        return static_cast<int>(res);
    }

    //! Remove layer and free space
    void remove_level(u16 level) {
        auto pred = [level] (const SubtreeRef& cur) {
            return cur.level != level;
        };
        auto sz = std::count_if(refs_.begin(), refs_.end(), pred);
        std::vector<SubtreeRef> newrefs;
        newrefs.reserve(static_cast<size_t>(sz));
        std::copy_if(refs_.begin(), refs_.end(), std::back_inserter(newrefs), pred);
        std::swap(refs_, newrefs);
    }
};

namespace {
u8* encode_subtree_ref(u8* dest, size_t dest_size, const SubtreeRef& ref) {
    auto end = dest + dest_size;
    auto put_u64 = [&](u64 val) {
        Base128Int<u64> enc(val);
        return enc.put(dest, end);
    };

    auto put_double = [&](double x) {
        // Put without compression
        union {
            double x;
            u8 bits[8];
        } value;
        value.x = x;
        if (end - dest > 8) {
            memcpy(dest, value.bits, 8);
            return dest + 8;
        }
        return dest;
    };

    // This macro checks output address and adjusts dest or returns
    #define CHECK_BOUNDS(exp) { \
        auto outp = exp; \
        if (outp == dest) { \
            return dest; \
        } else { \
            dest = outp; \
        }\
    }\

    // Put values to stream
    CHECK_BOUNDS(put_u64(ref.count));

    CHECK_BOUNDS(put_u64(ref.begin));

    auto dend = ref.end - ref.begin;
    CHECK_BOUNDS(put_u64(dend));

    CHECK_BOUNDS(put_u64(ref.addr));

    CHECK_BOUNDS(put_double(ref.min));

    auto dmin = ref.min_time - ref.begin;
    CHECK_BOUNDS(put_u64(dmin));

    CHECK_BOUNDS(put_double(ref.max));

    auto dmax = ref.max_time - ref.begin;
    CHECK_BOUNDS(put_u64(dmax));

    CHECK_BOUNDS(put_double(ref.sum));

    CHECK_BOUNDS(put_double(ref.first));

    CHECK_BOUNDS(put_double(ref.last));

    CHECK_BOUNDS(put_u64(static_cast<u64>(ref.type)));

    CHECK_BOUNDS(put_u64(ref.level));

    CHECK_BOUNDS(put_u64(ref.payload_size));

    CHECK_BOUNDS(put_u64(ref.fanout_index));

    CHECK_BOUNDS(put_u64(ref.checksum));

    return dest;
}
}

struct CompressedRefStorage {
    std::vector<u8> buffer_;

    void append(const SubtreeRef& ref) {
        encode_subtree_ref(buffer_.data(), buffer_.size(), ref);
        throw "not implemented";
    }
};

}
}
