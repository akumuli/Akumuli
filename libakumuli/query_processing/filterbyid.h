#pragma once

#include <memory>

#include "../queryprocessor_framework.h"
#include "../util.h"  // for panic

namespace Akumuli {
namespace QP {

/** Filter ids using predicate.
  * Predicate is an unary functor that accepts parameter of type aku_ParamId - fun(aku_ParamId) -> bool.
  */
template <class Predicate>
struct FilterByIdNode : std::enable_shared_from_this<FilterByIdNode<Predicate>>, Node {
    //! Id matching predicate
    Predicate             op_;
    std::shared_ptr<Node> next_;

    FilterByIdNode(Predicate pred, std::shared_ptr<Node> next)
        : op_(pred)
        , next_(next) {}

    virtual void complete() { next_->complete(); }

    virtual bool put(const aku_Sample& sample) {
        return op_(sample.paramid) ? next_->put(sample) : true;
    }

    void set_error(aku_Status status) {
        if (!next_) {
            AKU_PANIC("bad query processor node, next not set");
        }
        next_->set_error(status);
    }

    virtual int get_requirements() const { return EMPTY; }
};
}
}  // namespace
