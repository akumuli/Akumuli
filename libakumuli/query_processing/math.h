#pragma once

#include <memory>
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

template<class Op>
struct MathOperation : Node {

    std::shared_ptr<Node> next_;
    bool ignore_missing_;

    MathOperation(bool ignore_missing, std::shared_ptr<Node> next);

    MathOperation(const boost::property_tree::ptree&ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(MutableSample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

template<class Op>
MathOperation<Op>::MathOperation(bool ignore_missing, std::shared_ptr<Node> next)
    : next_(next)
    , ignore_missing_(ignore_missing)
{
}

template<class Op>
MathOperation<Op>::MathOperation(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    ignore_missing_ = ptree.get<bool>("ignore_missing");
}

template<class Op>
void MathOperation<Op>::complete() {
    next_->complete();
}

template<class Op>
bool MathOperation<Op>::put(MutableSample &mut) {
    Op operation;
    auto size = mut.size();
    double acc = 0.;
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        double x;
        if (value == nullptr) {
            if (ignore_missing_) {
                x = operation.unit();
            } else {
                x = std::numeric_limits<double>::quiet_NaN();
            }
        } else {
            x = *value;
        }
        acc = operation(acc, x);
    }
    mut.collapse();
    double* value = mut[0];
    *value = acc;
    return next_->put(mut);
}

template<class Op>
void MathOperation<Op>::set_error(aku_Status status) {
    next_->set_error(status);
}

template<class Op>
int MathOperation<Op>::get_requirements() const {
    return TERMINAL;
}
}
}  // namespace
