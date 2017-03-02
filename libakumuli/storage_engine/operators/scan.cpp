#include "scan.h"

namespace Akumuli {
namespace StorageEngine {

std::tuple<aku_Status, size_t> ChainOperator::read(aku_Timestamp *destts, double *destval, size_t size) {
    aku_Status status = AKU_ENO_DATA;
    size_t ressz = 0;  // current size
    size_t accsz = 0;  // accumulated size
    while(iter_index_ < iter_.size()) {
        std::tie(status, ressz) = iter_[iter_index_]->read(destts, destval, size);
        destts += ressz;
        destval += ressz;
        size -= ressz;
        accsz += ressz;
        if (size == 0) {
            break;
        }
        if (status == AKU_ENO_DATA || status == AKU_EUNAVAILABLE) {
            // this leaf node is empty or removed, continue with next
            iter_index_++;
            continue;
        }
        if (status != AKU_SUCCESS) {
            // Stop iteration or error!
            return std::tie(status, accsz);
        }
    }
    return std::tie(status, accsz);
}

RealValuedOperator::Direction ChainOperator::get_direction() {
    return dir_;
}


ChainMaterializer::ChainMaterializer(std::vector<aku_ParamId>&& ids, std::vector<std::unique_ptr<RealValuedOperator>>&& it)
    : iters_(std::move(it))
    , ids_(std::move(ids))
    , pos_(0)
{
}

std::tuple<aku_Status, size_t> ChainMaterializer::read(u8 *dest, size_t dest_size) {
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

}}  // namespace
