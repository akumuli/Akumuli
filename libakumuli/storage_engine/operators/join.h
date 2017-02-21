#pragma once

#include "operator.h"

// For MergeJoinOperator
#include <boost/heap/skew_heap.hpp>
#include <boost/range.hpp>
#include <boost/range/iterator_range.hpp>

namespace Akumuli {
namespace StorageEngine {


/** Operator that can be used to align several trees together
  */
struct JoinOperator : TupleOperator {

    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    aku_ParamId id_;
    static const size_t BUFFER_SIZE = 4096;
    static const size_t MAX_TUPLE_SIZE = 64;
    std::vector<std::vector<std::pair<aku_Timestamp, double>>> buffers_;
    u32 buffer_pos_;
    u32 buffer_size_;

    JoinOperator(std::vector<std::unique_ptr<RealValuedOperator>>&& iters, aku_ParamId id);

    aku_Status fill_buffers();

    /** Get pointer to buffer and return pointer to sample and tuple data */
    static std::tuple<aku_Sample*, double*> cast(u8* dest);

    /** Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};

/**
 * Merges several materialized tuple sequences into one
 */
struct MergeJoinOperator : TupleOperator {

    enum {
        RANGE_SIZE=1024
    };

    template<int dir>
    struct OrderByTimestamp {
        typedef std::tuple<aku_Timestamp, aku_ParamId> KeyType;
        struct HeapItem {
            KeyType key;
            aku_Sample const* sample;
            size_t index;
        };
        std::greater<KeyType> greater_;
        std::less<KeyType> less_;

        bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
            if (dir == 0) {
                return greater_(lhs.key, rhs.key);
            }
            return less_(lhs.key, rhs.key);
        }
    };

    struct Range {
        std::vector<u8> buffer;
        u32 size;
        u32 pos;
        u32 last_advance;

        Range()
            : size(0u)
            , pos(0u)
            , last_advance(0u)
        {
            buffer.resize(RANGE_SIZE*sizeof(aku_Sample));
        }

        void advance(u32 sz) {
            pos += sz;
            last_advance = sz;
        }

        void retreat() {
            assert(pos > last_advance);
            pos -= last_advance;
        }

        bool empty() const {
            return !(pos < size);
        }

        std::tuple<aku_Timestamp, aku_ParamId> top_key() const {
            u8 const* top = buffer.data() + pos;
            aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(top);
            return std::make_tuple(sample->timestamp, sample->paramid);
        }

        aku_Sample const* top() const {
            u8 const* top = buffer.data() + pos;
            return reinterpret_cast<aku_Sample const*>(top);
        }
    };

    std::vector<std::unique_ptr<TupleOperator>> iters_;
    bool forward_;
    std::vector<Range> ranges_;

    MergeJoinOperator(std::vector<std::unique_ptr<TupleOperator>>&& it, bool forward);

    virtual std::tuple<aku_Status, size_t> read(u8* dest, size_t size) override;

    template<int dir>
    std::tuple<aku_Status, size_t> kway_merge(u8* dest, size_t size) {
        if (iters_.empty()) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        size_t outpos = 0;
        if (ranges_.empty()) {
            // `ranges_` array should be initialized on first call
            for (size_t i = 0; i < iters_.size(); i++) {
                Range range;
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[i]->read(range.buffer.data(), range.buffer.size());
                if (status == AKU_SUCCESS || (status == AKU_ENO_DATA && outsize != 0)) {
                    range.size = static_cast<u32>(outsize);
                    range.pos  = 0;
                    ranges_.push_back(std::move(range));
                }
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
            }
        }

        typedef OrderByTimestamp<dir> Comp;
        typedef typename Comp::HeapItem HeapItem;
        typedef typename Comp::KeyType KeyType;
        typedef boost::heap::skew_heap<HeapItem, boost::heap::compare<Comp>> Heap;
        Heap heap;

        size_t index = 0;
        for(auto& range: ranges_) {
            if (!range.empty()) {
                KeyType key = range.top_key();
                heap.push({key, range.top(), index});
            }
            index++;
        }

        while(!heap.empty()) {
            HeapItem item = heap.top();
            size_t index = item.index;
            aku_Sample const* sample = item.sample;
            if (size - outpos >= sample->payload.size) {
                memcpy(dest + outpos, sample, sample->payload.size);
                outpos += sample->payload.size;
            } else {
                // Output buffer is fully consumed
                return std::make_tuple(AKU_SUCCESS, outpos);
            }
            heap.pop();
            ranges_[index].advance(sample->payload.size);
            if (ranges_[index].empty()) {
                // Refill range if possible
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[index]->read(ranges_[index].buffer.data(), ranges_[index].buffer.size());
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
                ranges_[index].size = static_cast<u32>(outsize);
                ranges_[index].pos  = 0;
            }
            if (!ranges_[index].empty()) {
                KeyType point = ranges_[index].top_key();
                heap.push({point, ranges_[index].top(), index});
            }
        }
        if (heap.empty()) {
            iters_.clear();
            ranges_.clear();
        }
        // All iterators are fully consumed
        return std::make_tuple(AKU_ENO_DATA, outpos);
    }

};

}
}
