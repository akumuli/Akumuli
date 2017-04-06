#include "merge.h"

namespace Akumuli {
namespace StorageEngine {


// Merge Join //

MergeJoinMaterializer::MergeJoinMaterializer(std::vector<std::unique_ptr<ColumnMaterializer>>&& it, bool forward)
    : iters_(std::move(it))
    , forward_(forward)
{
}

std::tuple<aku_Status, size_t> MergeJoinMaterializer::read(u8* dest, size_t size) {
    if (forward_) {
        return kway_merge<0>(dest, size);
    }
    return kway_merge<1>(dest, size);
}

}}  // namespace
