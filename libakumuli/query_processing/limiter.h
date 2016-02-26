#pragma once

#include <memory>

#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

struct Limiter : Node {

    uint64_t              limit_;
    uint64_t              offset_;
    uint64_t              counter_;
    std::shared_ptr<Node> next_;

    Limiter(uint64_t limit, uint64_t offset, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(const aku_Sample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};
}
}  // namespace
