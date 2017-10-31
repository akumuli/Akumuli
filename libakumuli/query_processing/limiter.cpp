#include "limiter.h"

namespace Akumuli {
namespace QP {

Limiter::Limiter(u64 limit, u64 offset, std::shared_ptr<Node> next)
    : limit_(limit)
    , offset_(offset)
    , counter_(0)
    , next_(next)
{
}

void Limiter::complete() {
    next_->complete();
}

bool Limiter::put(MutableSample &sample) {
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

