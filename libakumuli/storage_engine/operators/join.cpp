#include "join.h"

namespace Akumuli {
namespace StorageEngine {

static std::tuple<aku_Sample*, double*> cast(u8* dest) {
    aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
    double* tuple      = reinterpret_cast<double*>(sample->payload.data);
    return std::make_tuple(sample, tuple);
}

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


            //                       //
            //   JoinMaterializer2   //
            //                       //

JoinMaterializer2::JoinMaterializer2(std::vector<aku_ParamId>&& ids,
                                     std::vector<std::unique_ptr<RealValuedOperator>>&& iters,
                                     aku_ParamId id)
    : orig_ids_(ids)
    , id_(id)
    , curr_(AKU_MIN_TIMESTAMP)
    , max_ssize_(static_cast<u32>(sizeof(aku_Sample) + sizeof(double)*ids.size()))
{
    merge_.reset(new MergeMaterializer<TimeOrder>(std::move(ids), std::move(iters)));
    buffer_.resize(0x1000);
}

aku_Status JoinMaterializer2::fill_buffer() {
    auto beg = buffer_.begin();
    auto mid = buffer_.begin() + buffer_pos_;
    auto end = buffer_.begin() + buffer_size_;
    buffer_size_ = static_cast<u32>(end - mid);
    buffer_pos_  = 0;
    std::rotate(beg, mid, end);

    size_t bytes_written;
    aku_Status status;
    std::tie(status, bytes_written) = merge_->read(buffer_.data() + buffer_size_, buffer_.size() - buffer_size_);
    if (status == AKU_SUCCESS) {
        buffer_size_ += static_cast<u32>(bytes_written);
    }
    return status;
}

std::tuple<aku_Status, size_t> JoinMaterializer2::read(u8 *dest, size_t size) {
    size_t pos = 0;
    while(pos < (size - max_ssize_)) {
        aku_Sample* sample;
        double*     values;
        std::tie(sample, values) = cast(dest + pos);

        union {
            double d;
            u64    u;
        } ctrl;

        ctrl.u = 0;

        u32 tuple_pos = 0;

        for (u32 i = 0; i < orig_ids_.size(); i++) {
            if (buffer_size_ - buffer_pos_ < sizeof(aku_Sample)) {
                auto status = fill_buffer();
                if (status != AKU_SUCCESS) {
                    return std::make_tuple(status, 0);
                }
                if (buffer_size_ == 0) {
                    break;
                }
            }

            const aku_Sample* srcsample = reinterpret_cast<const aku_Sample*>(buffer_.data() + buffer_pos_);
            if (srcsample->paramid != orig_ids_.at(i)) {
                continue;
            }

            if (tuple_pos == 0) {
                // Expect curr_ to be different than srcsample->timestamp
                curr_ = srcsample->timestamp;
            } else if (curr_ != srcsample->timestamp) {
                break;
            }

            ctrl.u |= 1ul << i;
            values[tuple_pos] = srcsample->payload.float64;
            tuple_pos++;

            buffer_pos_ += srcsample->payload.size;
        }

        auto size = sizeof(aku_Sample) + tuple_pos*sizeof(double);
        pos += size;
        ctrl.u                 |= static_cast<u64>(orig_ids_.size()) << 58;
        sample->timestamp       = curr_;
        sample->paramid         = id_;
        sample->payload.float64 = ctrl.d;
        sample->payload.type    = AKU_PAYLOAD_TUPLE;
        sample->payload.size    = static_cast<u16>(size);
    }
    return std::make_tuple(AKU_SUCCESS, pos);
}

}}
