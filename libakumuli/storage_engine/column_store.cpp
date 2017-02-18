#include "column_store.h"
#include "log_iface.h"
#include "status_util.h"
#include "query_processing/queryparser.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/heap/skew_heap.hpp>
#include <boost/range.hpp>
#include <boost/range/iterator_range.hpp>

namespace Akumuli {
namespace StorageEngine {

using namespace QP;

static std::string to_string(ReshapeRequest const& req) {
    std::stringstream str;
    str << "ReshapeRequest(";
    switch (req.order_by) {
    case OrderBy::SERIES:
        str << "order-by: series, ";
        break;
    case OrderBy::TIME:
        str << "order-by: time, ";
        break;
    };
    if (req.group_by.enabled) {
        str << "group-by: enabled, ";
    } else {
        str << "group-by: disabled, ";
    }
    str << "range-begin: " << req.select.begin << ", range-end: " << req.select.end << ", ";
    str << "select: " << req.select.columns.size() << ")";
    return str.str();
}

/** This interface is used by column-store internally.
  * It allows to iterate through a bunch of columns row by row.
  */
struct RowIterator {

    virtual ~RowIterator() = default;

    /** Read samples in batch.
      * Samples can be of variable size.
      * @param dest is a pointer to buffer that will receive series of aku_Sample values
      * @param size is a size of the buffer in bytes
      * @return status of the operation (success or error code) and number of written bytes
      */
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) = 0;
};


class ChainIterator : public RowIterator {
    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
public:
    ChainIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it);
    virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};


ChainIterator::ChainIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it)
    : iters_(std::move(it))
    , ids_(std::move(ids))
    , pos_(0)
{
}

std::tuple<aku_Status, size_t> ChainIterator::read(u8 *dest, size_t dest_size) {
    aku_Status status = AKU_ENO_DATA;
    size_t ressz = 0;  // current size
    size_t accsz = 0;  // accumulated size
    size_t size = dest_size / sizeof(aku_Sample);
    std::vector<aku_Timestamp> destts_vec(size, 0);
    std::vector<double> destval_vec(size, 0);
    std::vector<aku_ParamId> outids(size, 0);
    aku_Timestamp* destts = destts_vec.data();
    double* destval = destval_vec.data();
    while(pos_ < iters_.size()) {
        aku_ParamId curr = ids_[pos_];
        std::tie(status, ressz) = iters_[pos_]->read(destts, destval, size);
        for (size_t i = accsz; i < accsz+ressz; i++) {
            outids[i] = curr;
        }
        destts += ressz;
        destval += ressz;
        size -= ressz;
        accsz += ressz;
        if (size == 0) {
            break;
        }
        pos_++;
        if (status == AKU_ENO_DATA) {
            // this iterator is done, continue with next
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Stop iteration on error!
            break;
        }
    }
    // Convert vectors to series of samples
    for (size_t i = 0; i < accsz; i++) {
        aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
        dest += sizeof(aku_Sample);
        sample->payload.type = AKU_PAYLOAD_FLOAT;
        sample->payload.size = sizeof(aku_Sample);
        sample->paramid = outids[i];
        sample->timestamp = destts_vec[i];
        sample->payload.float64 = destval_vec[i];
    }
    return std::make_tuple(status, accsz*sizeof(aku_Sample));
}


class Aggregator : public RowIterator {
    std::vector<std::unique_ptr<AggregateOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    size_t pos_;
    AggregationFunction func_;
public:
    Aggregator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<AggregateOperator>>&& it, AggregationFunction func)
        : iters_(std::move(it))
        , ids_(std::move(ids))
        , pos_(0)
        , func_(func)
    {
    }

    /**
     * @brief read data from iterators collection
     * @param dest is a destination for aggregate
     * @param size size of both array
     * @return status and number of elements in dest
     */
    std::tuple<aku_Status, size_t> read(u8* dest, size_t size) {
        aku_Status status = AKU_ENO_DATA;
        size_t nelements = 0;
        while(pos_ < iters_.size()) {
            aku_Timestamp destts = 0;
            NBTreeAggregationResult destval;
            size_t outsz = 0;
            std::tie(status, outsz) = iters_[pos_]->read(&destts, &destval, size);
            if (outsz == 0 && status == AKU_ENO_DATA) {
                // Move to next iterator
                pos_++;
                continue;
            }
            if (outsz != 1) {
                Logger::msg(AKU_LOG_TRACE, "Unexpected aggregate size " + std::to_string(outsz));
                continue;
            }
            // create sample
            aku_Sample sample;
            sample.paramid = ids_.at(pos_);
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.payload.size = sizeof(aku_Sample);
            switch (func_) {
            case AggregationFunction::MIN:
                sample.timestamp = destval.mints;
                sample.payload.float64 = destval.min;
            break;
            case AggregationFunction::MIN_TIMESTAMP:
                sample.timestamp = destval.mints;
                sample.payload.float64 = destval.mints;
            break;
            case AggregationFunction::MAX:
                sample.timestamp = destval.maxts;
                sample.payload.float64 = destval.max;
            break;
            case AggregationFunction::MAX_TIMESTAMP:
                sample.timestamp = destval.maxts;
                sample.payload.float64 = destval.maxts;
            break;
            case AggregationFunction::SUM:
                sample.timestamp = destval._end;
                sample.payload.float64 = destval.sum;
            break;
            case AggregationFunction::CNT:
                sample.timestamp = destval._end;
                sample.payload.float64 = destval.cnt;
            break;
            case AggregationFunction::MEAN:
                sample.timestamp = destval._end;
                sample.payload.float64 = destval.sum/destval.cnt;
            break;
            }
            memcpy(dest, &sample, sizeof(sample));
            // move to next
            nelements += 1;
            size -= sizeof(sample);
            dest += sizeof(sample);
            pos_++;
            if (size < sizeof(sample)) {
                break;
            }
            if (status == AKU_ENO_DATA) {
                // this iterator is done, continue with next
                continue;
            }
            if (status != AKU_SUCCESS) {
                // Stop iteration on error!
                break;
            }
        }
        return std::make_tuple(status, nelements*sizeof(aku_Sample));
    }
};


static const size_t RANGE_SIZE = 1024;


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

template<int dir>  // 0 - forward, 1 - backward
struct SeriesOrder {
    typedef std::tuple<aku_Timestamp, aku_ParamId> KeyType;
    typedef std::tuple<KeyType, double, u32> HeapItem;
    typedef std::tuple<aku_ParamId, aku_Timestamp> InvKeyType;
    std::greater<InvKeyType> greater_;
    std::less<InvKeyType> less_;

    bool operator () (HeapItem const& lhs, HeapItem const& rhs) const {
        auto lkey = std::get<0>(lhs);
        InvKeyType ilhs = std::make_tuple(std::get<1>(lkey), std::get<0>(lkey));
        auto rkey = std::get<0>(rhs);
        InvKeyType irhs = std::make_tuple(std::get<1>(rkey), std::get<0>(rkey));
        if (dir == 0) {
            return greater_(ilhs, irhs);
        }
        return less_(ilhs, irhs);
    }
};


template<template <int dir> class CmpPred>
struct MergeIterator : RowIterator {
    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    std::vector<aku_ParamId> ids_;
    bool forward_;

    struct Range {
        std::vector<aku_Timestamp> ts;
        std::vector<double> xs;
        aku_ParamId id;
        size_t size;
        size_t pos;

        Range(aku_ParamId id)
            : id(id)
            , size(0)
            , pos(0)
        {
            ts.resize(RANGE_SIZE);
            xs.resize(RANGE_SIZE);
        }

        void advance() {
            pos++;
        }

        void retreat() {
            assert(pos);
            pos--;
        }

        bool empty() const {
            return !(pos < size);
        }

        std::tuple<aku_Timestamp, aku_ParamId> top_key() const {
            return std::make_tuple(ts.at(pos), id);
        }

        double top_value() const {
            return xs.at(pos);
        }
    };

    std::vector<Range> ranges_;

    MergeIterator(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it)
        : iters_(std::move(it))
        , ids_(std::move(ids))
    {
        if (!iters_.empty()) {
            forward_ = iters_.front()->get_direction() == RealValuedOperator::Direction::FORWARD;
        }
        if (iters_.size() != ids_.size()) {
            AKU_PANIC("MergeIterator - broken invariant");
        }
    }

    virtual std::tuple<aku_Status, size_t> read(u8* dest, size_t size) override {
        if (forward_) {
            return kway_merge<0>(dest, size);
        }
        return kway_merge<1>(dest, size);
    }

    template<int dir>
    std::tuple<aku_Status, size_t> kway_merge(u8* dest, size_t size) {
        if (iters_.empty()) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
        size_t outpos = 0;
        if (ranges_.empty()) {
            // `ranges_` array should be initialized on first call
            for (size_t i = 0; i < iters_.size(); i++) {
                Range range(ids_[i]);
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[i]->read(range.ts.data(), range.xs.data(), RANGE_SIZE);
                if (status == AKU_SUCCESS || (status == AKU_ENO_DATA && outsize != 0)) {
                    range.size = outsize;
                    range.pos  = 0;
                    ranges_.push_back(std::move(range));
                }
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
            }
        }

        typedef CmpPred<dir> Comp;
        typedef typename Comp::HeapItem HeapItem;
        typedef typename Comp::KeyType KeyType;
        typedef boost::heap::skew_heap<HeapItem, boost::heap::compare<Comp>> Heap;
        Heap heap;

        int index = 0;
        for(auto& range: ranges_) {
            if (!range.empty()) {
                KeyType key = range.top_key();
                heap.push(std::make_tuple(key, range.top_value(), index));
            }
            index++;
        }

        enum {
            KEY = 0,
            VALUE = 1,
            INDEX = 2,
            TIME = 0,
            ID = 1,
        };

        while(!heap.empty()) {
            HeapItem item = heap.top();
            KeyType point = std::get<KEY>(item);
            u32 index = std::get<INDEX>(item);
            aku_Sample sample;
            sample.paramid = std::get<ID>(point);
            sample.timestamp = std::get<TIME>(point);
            sample.payload.type = AKU_PAYLOAD_FLOAT;
            sample.payload.size = sizeof(aku_Sample);
            sample.payload.float64 = std::get<VALUE>(item);
            if (size - outpos >= sizeof(aku_Sample)) {
                memcpy(dest + outpos, &sample, sizeof(sample));
                outpos += sizeof(sample);
            } else {
                // Output buffer is fully consumed
                return std::make_tuple(AKU_SUCCESS, size);
            }
            heap.pop();
            ranges_[index].advance();
            if (ranges_[index].empty()) {
                // Refill range if possible
                aku_Status status;
                size_t outsize;
                std::tie(status, outsize) = iters_[index]->read(ranges_[index].ts.data(), ranges_[index].xs.data(), RANGE_SIZE);
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
                ranges_[index].size = outsize;
                ranges_[index].pos  = 0;
            }
            if (!ranges_[index].empty()) {
                KeyType point = ranges_[index].top_key();
                heap.push(std::make_tuple(point, ranges_[index].top_value(), index));
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

/** Iterator used to align several trees together
  */
struct JoinIterator : RowIterator {

    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    aku_ParamId id_;
    static const size_t BUFFER_SIZE = 4096;
    static const size_t MAX_TUPLE_SIZE = 64;
    std::vector<std::vector<std::pair<aku_Timestamp, double>>> buffers_;
    u32 buffer_pos_;
    u32 buffer_size_;

    JoinIterator(std::vector<std::unique_ptr<RealValuedOperator>>&& iters, aku_ParamId id)
        : iters_(std::move(iters))
        , id_(id)
        , buffer_pos_(0)
        , buffer_size_(0)
    {
        if (iters.size() > MAX_TUPLE_SIZE) {
            AKU_PANIC("Invalid join");
        }
        auto ncol = iters_.size();
        buffers_.resize(ncol);
        for(u32 i = 0; i < iters_.size(); i++) {
            buffers_.at(i).resize(BUFFER_SIZE);
        }
    }

    aku_Status fill_buffers() {
        if (buffer_pos_ != buffer_size_) {
            // Logic error
            AKU_PANIC("Buffers are not consumed");
        }
        aku_Timestamp destts[BUFFER_SIZE];
        double destval[BUFFER_SIZE];
        std::vector<u32> sizes;
        size_t ixbuf = 0;
        for (auto const& it: iters_) {
            aku_Status status;
            size_t size;
            std::tie(status, size) = it->read(destts, destval, BUFFER_SIZE);
            if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                return status;
            }
            for (size_t i = 0; i < size; i++) {
                buffers_[ixbuf][i] = std::make_pair(destts[i], destval[i]);
            }
            ixbuf++;
            sizes.push_back(static_cast<u32>(size));  // safe to cast because size < BUFFER_SIZE
        }
        buffer_pos_ = 0;
        buffer_size_ = sizes.front();
        for(auto sz: sizes) {
            if (sz != buffer_size_) {
                return AKU_EBAD_DATA;
            }
        }
        if (buffer_size_ == 0) {
            return AKU_ENO_DATA;
        }
        return AKU_SUCCESS;
    }

    /** Get pointer to buffer and return pointer to sample and tuple data */
    static std::tuple<aku_Sample*, double*> cast(u8* dest) {
        aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
        double* tuple      = reinterpret_cast<double*>(sample->payload.data);
        return std::make_tuple(sample, tuple);
    }

    /** Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) {
        aku_Status status      = AKU_SUCCESS;
        size_t ncolumns        = iters_.size();
        size_t max_sample_size = sizeof(aku_Sample) + ncolumns;
        size_t output_size     = 0;

        while(size >= max_sample_size) {
            // Fill buffers
            if (buffer_pos_ == buffer_size_) {
                // buffers consumed (or not used yet)
                status = fill_buffers();
                if (status != AKU_SUCCESS) {
                    return std::make_tuple(status, output_size);
                }
            }
            // Allocate new element inside `dest` array
            u64 bitmap = 1;
            aku_Sample* sample;
            double* tuple;
            std::tie(sample, tuple) = cast(dest);

            tuple[0] = buffers_[0][buffer_pos_].second;
            aku_Timestamp key = buffers_[0][buffer_pos_].first;
            u32 nelements = 1;

            for (u32 i = 1; i < ncolumns; i++) {
                aku_Timestamp ts = buffers_[i][buffer_pos_].first;
                if (ts == key) {
                    // value is found
                    double val = buffers_[i][buffer_pos_].second;
                    tuple[i] = val;
                    bitmap |= (1 << i);
                    nelements += 1;
                }
            }
            buffer_pos_++;
            union {
                u64 u;
                double d;
            } bits;
            bits.u = bitmap;
            size_t sample_size      = sizeof(aku_Sample) + sizeof(double)*nelements;
            sample->paramid         = id_;
            sample->timestamp       = key;
            sample->payload.size    = static_cast<u16>(sample_size);
            sample->payload.type    = AKU_PAYLOAD_TUPLE;
            sample->payload.float64 = bits.d;
            size                   -= sample_size;
            dest                   += sample_size;
            output_size            += sample_size;
        }
        return std::make_tuple(AKU_SUCCESS, output_size);
    }
};


// Merge-Join //

struct MergeJoinIterator : RowIterator {

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

    std::vector<std::unique_ptr<RowIterator>> iters_;
    bool forward_;
    std::vector<Range> ranges_;

    MergeJoinIterator(std::vector<std::unique_ptr<RowIterator>>&& it, bool forward)
        : iters_(std::move(it))
        , forward_(forward)
    {
    }

    virtual std::tuple<aku_Status, size_t> read(u8* dest, size_t size) override {
        if (forward_) {
            return kway_merge<0>(dest, size);
        }
        return kway_merge<1>(dest, size);
    }

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


namespace GroupAggregate {

    struct TupleOutputUtils {
        /** Get pointer to buffer and return pointer to sample and tuple data */
        static std::tuple<aku_Sample*, double*> cast(u8* dest) {
            aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
            double* tuple      = reinterpret_cast<double*>(sample->payload.data);
            return std::make_tuple(sample, tuple);
        }

        static double get_flags(std::vector<AggregationFunction> const& tup) {
            // Shift will produce power of two (e.g. if tup.size() == 3 then
            // (1 << tup.size) will give us 8, 8-1 is 7 (exactly three lower
            // bits is set)).
            union {
                double d;
                u64 u;
            } bits;
            bits.u = (1ull << tup.size()) - 1;
            return bits.d;
        }

        static double get(NBTreeAggregationResult const& res, AggregationFunction afunc) {
            double out = 0;
            switch (afunc) {
            case AggregationFunction::CNT:
                out = res.cnt;
                break;
            case AggregationFunction::SUM:
                out = res.sum;
                break;
            case AggregationFunction::MIN:
                out = res.min;
                break;
            case AggregationFunction::MIN_TIMESTAMP:
                out = static_cast<double>(res.mints);
                break;
            case AggregationFunction::MAX:
                out = res.max;
                break;
            case AggregationFunction::MAX_TIMESTAMP:
                out = res.maxts;
                break;
            case AggregationFunction::MEAN:
                out = res.sum / res.cnt;
                break;
            }
            return out;
        }

        static void set_tuple(double* tuple, std::vector<AggregationFunction> const& comp, NBTreeAggregationResult const& res) {
            for (size_t i = 0; i < comp.size(); i++) {
                auto elem = comp[i];
                *tuple = get(res, elem);
                tuple++;
            }
        }

        static size_t get_tuple_size(const std::vector<AggregationFunction>& tup) {
            size_t payload = 0;
            assert(!tup.empty());
            payload = sizeof(double)*tup.size();
            return sizeof(aku_Sample) + payload;
        }
    };

    struct SeriesOrderIterator : TupleOutputUtils, RowIterator {
        std::vector<std::unique_ptr<AggregateOperator>> iters_;
        std::vector<aku_ParamId> ids_;
        std::vector<AggregationFunction> tuple_;
        u32 pos_;

        SeriesOrderIterator(std::vector<aku_ParamId>&& ids,
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

    std::tuple<aku_Status, size_t> SeriesOrderIterator::read(u8 *dest, size_t dest_size) {
        aku_Status status = AKU_ENO_DATA;
        size_t ressz = 0;  // current size
        size_t accsz = 0;  // accumulated size
        size_t sample_size = get_tuple_size(tuple_);
        size_t size = dest_size / sample_size;
        std::vector<aku_Timestamp> destts_vec(size, 0);
        std::vector<NBTreeAggregationResult> destval_vec(size, INIT_AGGRES);
        std::vector<aku_ParamId> outids(size, 0);
        aku_Timestamp* destts = destts_vec.data();
        NBTreeAggregationResult* destval = destval_vec.data();
        while(pos_ < iters_.size()) {
            aku_ParamId curr = ids_[pos_];
            std::tie(status, ressz) = iters_[pos_]->read(destts, destval, size);
            for (size_t i = accsz; i < accsz+ressz; i++) {
                outids[i] = curr;
            }
            destts += ressz;
            destval += ressz;
            size -= ressz;
            accsz += ressz;
            if (size == 0) {
                break;
            }
            pos_++;
            if (status == AKU_ENO_DATA) {
                // this iterator is done, continue with next
                continue;
            }
            if (status != AKU_SUCCESS) {
                // Stop iteration on error!
                break;
            }
        }
        // Convert vectors to series of samples
        for (size_t i = 0; i < accsz; i++) {
            double* tup;
            aku_Sample* sample;
            std::tie(sample, tup)   = cast(dest);
            dest                   += sample_size;
            sample->payload.type    = AKU_PAYLOAD_TUPLE;
            sample->payload.size    = static_cast<u16>(sample_size);
            sample->paramid         = outids[i];
            sample->timestamp       = destts_vec[i];
            sample->payload.float64 = get_flags(tuple_);
            set_tuple(tup, tuple_, destval_vec[i]);
        }
        return std::make_tuple(status, accsz*sample_size);

    }

    struct TimeOrderIterator : TupleOutputUtils, RowIterator {
        std::unique_ptr<MergeJoinIterator> join_iter_;

        TimeOrderIterator(const std::vector<aku_ParamId>& ids,
                          std::vector<std::unique_ptr<AggregateOperator>> &it,
                          const std::vector<AggregationFunction>& components)
        {
            assert(it.size());
            bool forward = it.front()->get_direction() == AggregateOperator::Direction::FORWARD;
            std::vector<std::unique_ptr<RowIterator>> iters;
            for (size_t i = 0; i < ids.size(); i++) {
                std::unique_ptr<RowIterator> iter;
                auto agg = std::move(it.at(i));
                std::vector<std::unique_ptr<AggregateOperator>> agglist;
                agglist.push_back(std::move(agg));
                auto ptr = new SeriesOrderIterator({ ids[i] }, std::move(agglist), components);
                iter.reset(ptr);
                iters.push_back(std::move(iter));
            }
            join_iter_.reset(new MergeJoinIterator(std::move(iters), forward));
        }

        virtual std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) override {
            return join_iter_->read(dest, size);
        }
    };
}


// ////////////// //
//  Column-store  //
// ////////////// //

ColumnStore::ColumnStore(std::shared_ptr<BlockStore> bstore)
    : blockstore_(bstore)
{
}

aku_Status ColumnStore::open_or_restore(std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> const& mapping) {
    for (auto it: mapping) {
        aku_ParamId id = it.first;
        std::vector<LogicAddr> const& rescue_points = it.second;
        if (rescue_points.empty()) {
            AKU_PANIC("Invalid rescue points state");
        }
        auto status = NBTreeExtentsList::repair_status(rescue_points);
        if (status == NBTreeExtentsList::RepairStatus::REPAIR) {
            Logger::msg(AKU_LOG_ERROR, "Repair needed, id=" + std::to_string(id));
        }
        auto tree = std::make_shared<NBTreeExtentsList>(id, rescue_points, blockstore_);

        std::lock_guard<std::mutex> tl(table_lock_);
        if (columns_.count(id)) {
            Logger::msg(AKU_LOG_ERROR, "Can't open/repair " + std::to_string(id) + " (already exists)");
            return AKU_EBAD_ARG;
        } else {
            columns_[id] = std::move(tree);
        }
        columns_[id]->force_init();
    }
    return AKU_SUCCESS;
}

std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> ColumnStore::close() {
    std::unordered_map<aku_ParamId, std::vector<StorageEngine::LogicAddr>> result;
    std::lock_guard<std::mutex> tl(table_lock_);
    Logger::msg(AKU_LOG_INFO, "Column-store commit called");
    for (auto it: columns_) {
        auto addrlist = it.second->close();
        result[it.first] = addrlist;
    }
    Logger::msg(AKU_LOG_INFO, "Column-store commit completed");
    return result;
}

aku_Status ColumnStore::create_new_column(aku_ParamId id) {
    std::vector<LogicAddr> empty;
    auto tree = std::make_shared<NBTreeExtentsList>(id, empty, blockstore_);
    {
        std::lock_guard<std::mutex> tl(table_lock_);
        if (columns_.count(id)) {
            return AKU_EBAD_ARG;
        } else {
            columns_[id] = std::move(tree);
            columns_[id]->force_init();
            return AKU_SUCCESS;
        }
    }
}


void ColumnStore::query(const ReshapeRequest &req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `select` query: " + to_string(req));

    // Query validations
    if (req.select.columns.size() > 1) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `select` request, too many columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    } else if (req.select.columns.size() == 0) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `select` request, no columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    if (req.agg.enabled) {
        if (req.agg.func.size() > 1) {
            Logger::msg(AKU_LOG_ERROR, "Bad column-store `aggregate` request, too many aggregation functions (not yet supported)");
            qproc.set_error(AKU_EBAD_ARG);
            return;
        } else if (req.agg.func.empty()) {
            Logger::msg(AKU_LOG_ERROR, "Bad column-store `aggregate` request, aggregation function is not set");
            qproc.set_error(AKU_EBAD_ARG);
            return;
        }
    }

    std::unique_ptr<RowIterator> iter;
    auto ids = req.select.columns.at(0).ids;
    if (req.agg.enabled) {
        std::vector<std::unique_ptr<AggregateOperator>> agglist;
        for (auto id: req.select.columns.at(0).ids) {
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<AggregateOperator> agg = it->second->aggregate(req.select.begin, req.select.end);
                agglist.push_back(std::move(agg));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        if (req.group_by.enabled) {
            // FIXME: Not yet supported
            Logger::msg(AKU_LOG_ERROR, "Group-by in `aggregate` query is not supported yet");
            qproc.set_error(AKU_ENOT_PERMITTED);
            return;
        } else {
            if (req.order_by == OrderBy::SERIES) {
                iter.reset(new Aggregator(std::move(ids), std::move(agglist), req.agg.func.front()));
            } else {
                // Error: invalid query
                Logger::msg(AKU_LOG_ERROR, "Bad `aggregate` query, order-by statement not supported");
                qproc.set_error(AKU_ENOT_PERMITTED);
                return;
            }
        }
    } else {
        std::vector<std::unique_ptr<RealValuedOperator>> iters;
        for (auto id: req.select.columns.at(0).ids) {
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<RealValuedOperator> iter = it->second->search(req.select.begin, req.select.end);
                iters.push_back(std::move(iter));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        if (req.group_by.enabled) {
            // Transform each id
            for (size_t i = 0; i < ids.size(); i++) {
                auto oldid = ids[i];
                auto it = req.group_by.transient_map.find(oldid);
                if (it != req.group_by.transient_map.end()) {
                    ids[i] = it->second;
                } else {
                    // Bad transient id mapping found!
                    qproc.set_error(AKU_ENOT_FOUND);
                    return;
                }
            }
            if (req.order_by == OrderBy::SERIES) {
                iter.reset(new MergeIterator<SeriesOrder>(std::move(ids), std::move(iters)));
            } else {
                iter.reset(new MergeIterator<TimeOrder>(std::move(ids), std::move(iters)));
            }
        } else {
            if (req.order_by == OrderBy::SERIES) {
                iter.reset(new ChainIterator(std::move(ids), std::move(iters)));
            } else {
                iter.reset(new MergeIterator<TimeOrder>(std::move(ids), std::move(iters)));
            }
        }
    }

    const size_t dest_size = 0x1000;
    std::vector<aku_Sample> dest;
    dest.resize(dest_size);
    aku_Status status = AKU_SUCCESS;
    while(status == AKU_SUCCESS) {
        size_t size;
        // This is OK because normal query (aggregate or select) will write fixed size samples with size = sizeof(aku_Sample).
        //
        std::tie(status, size) = iter->read(reinterpret_cast<u8*>(dest.data()), dest_size*sizeof(aku_Sample));
        if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
            Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
            qproc.set_error(status);
            return;
        }
        size_t ixsize = size / sizeof(aku_Sample);
        for (size_t ix = 0; ix < ixsize; ix++) {
            if (!qproc.put(dest[ix])) {
                Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                return;
            }
        }
    }
}

void ColumnStore::join_query(QP::ReshapeRequest const& req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `json` query: " + to_string(req));
    if (req.select.columns.size() < 2) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `join` request, not enough columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    std::vector<std::unique_ptr<RowIterator>> iters;
    for (u32 ix = 0; ix < req.select.columns.front().ids.size(); ix++) {
        std::vector<std::unique_ptr<RealValuedOperator>> row;
        std::vector<aku_ParamId> ids;
        for (u32 col = 0; col < req.select.columns.size(); col++) {
            auto id = req.select.columns[col].ids[ix];
            ids.push_back(id);
            std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
            auto it = columns_.find(id);
            if (it != columns_.end()) {
                std::unique_ptr<RealValuedOperator> iter = it->second->search(req.select.begin, req.select.end);
                row.push_back(std::move(iter));
            } else {
                qproc.set_error(AKU_ENOT_FOUND);
                return;
            }
        }
        auto it = new JoinIterator(std::move(row), ids.front());
        iters.push_back(std::unique_ptr<JoinIterator>(it));
    }

    if (req.order_by == OrderBy::SERIES) {
        for (auto& it: iters) {
            const size_t dest_size = 4096;
            std::vector<u8> dest;
            dest.resize(dest_size);
            aku_Status status = AKU_SUCCESS;
            while(status == AKU_SUCCESS) {
                size_t size;
                std::tie(status, size) = it->read(dest.data(), dest_size);
                if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
                    Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
                    qproc.set_error(status);
                    return;
                }
                // Parse `dest` buffer
                u8 const* ptr = dest.data();
                u8 const* end = ptr + size;
                while (ptr < end) {
                    aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(ptr);
                    if (!qproc.put(*sample)) {
                        Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                        return;
                    }
                    ptr += sample->payload.size;
                }
            }
        }
    } else {
        std::unique_ptr<RowIterator> iter;
        bool forward = req.select.begin < req.select.end;
        iter.reset(new MergeJoinIterator(std::move(iters), forward));

        const size_t dest_size = 0x1000;
        std::vector<u8> dest;
        dest.resize(dest_size);
        aku_Status status = AKU_SUCCESS;
        while(status == AKU_SUCCESS) {
            size_t size;
            std::tie(status, size) = iter->read(dest.data(), dest_size);
            if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
                Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
                qproc.set_error(status);
                return;
            }
            size_t pos = 0;
            while(pos < size) {
                aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(dest.data() + pos);
                if (!qproc.put(*sample)) {
                    Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                    return;
                }
                pos += sample->payload.size;
            }
        }
    }
}

void ColumnStore::group_aggregate_query(QP::ReshapeRequest const& req, QP::IStreamProcessor& qproc) {
    Logger::msg(AKU_LOG_TRACE, "ColumnStore `json` query: " + to_string(req));
    if (req.select.columns.size() > 1) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `group-aggregate` request, too many columns");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    if (!req.agg.enabled || req.agg.step == 0) {
        Logger::msg(AKU_LOG_ERROR, "Bad column-store `group-aggregate` request, aggregation disabled");
        qproc.set_error(AKU_EBAD_ARG);
        return;
    }
    std::vector<std::unique_ptr<AggregateOperator>> agglist;
    for (auto id: req.select.columns.at(0).ids) {
        std::lock_guard<std::mutex> lg(table_lock_); AKU_UNUSED(lg);
        auto it = columns_.find(id);
        if (it != columns_.end()) {
            std::unique_ptr<AggregateOperator> agg = it->second->group_aggregate(req.select.begin, req.select.end, req.agg.step);
            agglist.push_back(std::move(agg));
        } else {
            qproc.set_error(AKU_ENOT_FOUND);
            return;
        }
    }
    std::unique_ptr<RowIterator> iter;
    auto ids = req.select.columns.at(0).ids;
    if (req.group_by.enabled) {
        // FIXME: Not yet supported
        Logger::msg(AKU_LOG_ERROR, "Group-by in `group-aggregate` query is not supported yet");
        qproc.set_error(AKU_ENOT_PERMITTED);
        return;
    } else {
        if (req.order_by == OrderBy::SERIES) {
            iter.reset(new GroupAggregate::SeriesOrderIterator(std::move(ids), std::move(agglist), req.agg.func));
        } else {
            iter.reset(new GroupAggregate::TimeOrderIterator(ids, agglist, req.agg.func));
        }
    }
    const size_t dest_size = 0x1000;
    std::vector<u8> dest;
    dest.resize(dest_size);
    aku_Status status = AKU_SUCCESS;
    while(status == AKU_SUCCESS) {
        size_t size;
        std::tie(status, size) = iter->read(dest.data(), dest_size);
        if (status != AKU_SUCCESS && (status != AKU_ENO_DATA && status != AKU_EUNAVAILABLE)) {
            Logger::msg(AKU_LOG_ERROR, "Iteration error " + StatusUtil::str(status));
            qproc.set_error(status);
            return;
        }
        size_t pos = 0;
        while(pos < size) {
            aku_Sample const* sample = reinterpret_cast<aku_Sample const*>(dest.data() + pos);
            if (!qproc.put(*sample)) {
                Logger::msg(AKU_LOG_TRACE, "Iteration stopped by client");
                return;
            }
            pos += sample->payload.size;
        }
    }
}

size_t ColumnStore::_get_uncommitted_memory() const {
    std::lock_guard<std::mutex> guard(table_lock_);
    size_t total_size = 0;
    for (auto const& p: columns_) {
        total_size += p.second->_get_uncommitted_size();
    }
    return total_size;
}

NBTreeAppendResult ColumnStore::write(aku_Sample const& sample, std::vector<LogicAddr>* rescue_points,
                               std::unordered_map<aku_ParamId, std::shared_ptr<NBTreeExtentsList>>* cache_or_null)
{
    std::lock_guard<std::mutex> lock(table_lock_);
    aku_ParamId id = sample.paramid;
    auto it = columns_.find(id);
    if (it != columns_.end()) {
        auto tree = it->second;
        auto res = tree->append(sample.timestamp, sample.payload.float64);
        if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            auto tmp = tree->get_roots();
            rescue_points->swap(tmp);
        }
        if (cache_or_null != nullptr) {
            cache_or_null->insert(std::make_pair(id, tree));
        }
        return res;
    }
    return NBTreeAppendResult::FAIL_BAD_ID;
}


// ////////////////////// //
//      WriteSession      //
// ////////////////////// //

CStoreSession::CStoreSession(std::shared_ptr<ColumnStore> registry)
    : cstore_(registry)
{
}

NBTreeAppendResult CStoreSession::write(aku_Sample const& sample, std::vector<LogicAddr> *rescue_points) {
    if (AKU_UNLIKELY(sample.payload.type != AKU_PAYLOAD_FLOAT)) {
        return NBTreeAppendResult::FAIL_BAD_VALUE;
    }
    // Cache lookup
    auto it = cache_.find(sample.paramid);
    if (it != cache_.end()) {
        auto res = it->second->append(sample.timestamp, sample.payload.float64);
        if (res == NBTreeAppendResult::OK_FLUSH_NEEDED) {
            auto tmp = it->second->get_roots();
            rescue_points->swap(tmp);
        }
        return res;
    }
    // Cache miss - access global registry
    return cstore_->write(sample, rescue_points, &cache_);
}

void CStoreSession::query(const ReshapeRequest &req, QP::IStreamProcessor& proc) {
    cstore_->query(req, proc);
}

}}  // namespace
