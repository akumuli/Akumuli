#include "join.h"

namespace Akumuli {
namespace StorageEngine {

static std::tuple<aku_Sample*, double*> cast(u8* dest) {
    aku_Sample* sample = reinterpret_cast<aku_Sample*>(dest);
    double* tuple      = reinterpret_cast<double*>(sample->payload.data);
    return std::make_tuple(sample, tuple);
}

            //                      //
            //   JoinMaterializer   //
            //                      //

JoinMaterializer::JoinMaterializer(std::vector<aku_ParamId>&& ids,
                                     std::vector<std::unique_ptr<RealValuedOperator>>&& iters,
                                     aku_ParamId id)
    : orig_ids_(ids)
    , id_(id)
    , curr_(AKU_MIN_TIMESTAMP)
    , buffer_size_(0)
    , buffer_pos_(0)
    , max_ssize_(static_cast<u32>(sizeof(aku_Sample) + sizeof(double)*ids.size()))
{
    merge_.reset(new MergeMaterializer<MergeJoinOrder, true>(std::move(ids), std::move(iters)));
    buffer_.resize(0x1000);
}

aku_Status JoinMaterializer::fill_buffer() {
    // Invariant: buffer_pos_ <= buffer_size_, buffer_size_ <= buffer_.size()
    assert(buffer_pos_ <= buffer_size_);
    assert(buffer_size_ <= buffer_.size());
    auto beg = buffer_.begin();
    auto mid = buffer_.begin() + buffer_pos_;
    auto end = buffer_.begin() + buffer_size_;
    buffer_size_ = static_cast<u32>(end - mid);
    buffer_pos_  = 0;
    std::rotate(beg, mid, end);

    size_t bytes_written;
    aku_Status status;
    std::tie(status, bytes_written) = merge_->read(buffer_.data() + buffer_size_, buffer_.size() - buffer_size_);
    if (status == AKU_SUCCESS || status == AKU_ENO_DATA) {
        buffer_size_ += static_cast<u32>(bytes_written);
    }
    return status;
}

std::tuple<aku_Status, size_t> JoinMaterializer::read(u8 *dest, size_t size) {
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
                if (status != AKU_SUCCESS && status != AKU_ENO_DATA) {
                    return std::make_tuple(status, 0);
                }
                if (buffer_size_ == 0) {
                    return std::make_tuple(AKU_ENO_DATA, pos);
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

            ctrl.u |= 1ull << i;
            values[tuple_pos] = srcsample->payload.float64;
            tuple_pos++;

            buffer_pos_ += srcsample->payload.size;
            assert(buffer_pos_ <= buffer_.size());
        }

        auto outsize            = sizeof(aku_Sample) + tuple_pos*sizeof(double);
        pos                    += outsize;
        ctrl.u                 |= static_cast<u64>(orig_ids_.size()) << 58;
        sample->timestamp       = curr_;
        sample->paramid         = id_;
        sample->payload.float64 = ctrl.d;
        sample->payload.type    = AKU_PAYLOAD_TUPLE;
        sample->payload.size    = static_cast<u16>(outsize);
    }
    return std::make_tuple(AKU_SUCCESS, pos);
}

}}
