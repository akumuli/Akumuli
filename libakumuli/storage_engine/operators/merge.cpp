#include "merge.h"

namespace Akumuli {
namespace StorageEngine {


// Merge Join //

MergeJoinOperator::MergeJoinOperator(std::vector<std::unique_ptr<TupleOperator>>&& it, bool forward)
    : iters_(std::move(it))
    , forward_(forward)
{
}

std::tuple<aku_Status, size_t> MergeJoinOperator::read(u8* dest, size_t size) {
    if (forward_) {
        return kway_merge<0>(dest, size);
    }
    return kway_merge<1>(dest, size);
}

}}  // namespace
