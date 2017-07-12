#include "join.h"

namespace Akumuli {
namespace StorageEngine {

JoinMaterializer::JoinMaterializer(std::vector<std::unique_ptr<RealValuedOperator>>&& iters, aku_ParamId id)
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

aku_Status JoinMaterializer::fill_buffers() {
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

std::tuple<aku_Sample*, double*> JoinMaterializer::cast(u8* dest) {
    aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
    double* tuple      = reinterpret_cast<double*>(sample->payload.data);
    return std::make_tuple(sample, tuple);
}

std::tuple<aku_Status, size_t> JoinMaterializer::read(u8 *dest, size_t size) {
    aku_Status status      = AKU_SUCCESS;
    size_t ncolumns        = iters_.size();
    size_t max_sample_size = sizeof(aku_Sample) + sizeof(double)*ncolumns;
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

}}
