#include "limiter.h"

namespace Akumuli {
namespace QP {

Limiter::Limiter(uint64_t limit, uint64_t offset, std::shared_ptr<Node> next)
    : limit_(limit)
    , offset_(offset)
    , counter_(0)
    , next_(next)
{
}

void Limiter::complete() {
    next_->complete();
}

bool Limiter::put(const aku_Sample& sample) {
    if (sample.payload.type > aku_PData::MARGIN || sample.payload.type == aku_PData::EMPTY) {
        // If margin or empty - continue
        return true;
    }
    if (counter_ < offset_) {
        // continue iteration
        return true;
    } else if (counter_ >= limit_) {
        // stop iteration
        return false;
    }
    counter_++;
    return next_->put(sample);
}

void Limiter::set_error(aku_Status status) {
    next_->set_error(status);
}

int Limiter::get_requirements() const {
    return TERMINAL;
}

}}  // namespace

