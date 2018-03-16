#pragma once

#include <memory>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

struct Limiter : Node {

    u64                   limit_;
    u64                   offset_;
    u64                   counter_;
    std::shared_ptr<Node> next_;

    Limiter(u64 limit, u64 offset, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};
}
}  // namespace
