#include "scan.h"

namespace Akumuli {
namespace StorageEngine {

std::tuple<aku_Status, size_t> ScanOperator::read(aku_Timestamp *destts, double *destval, size_t size) {
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

RealValuedOperator::Direction ScanOperator::get_direction() {
    return dir_;
}

}}  // namespace
