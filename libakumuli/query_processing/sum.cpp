#include "sum.h"

namespace Akumuli {
namespace QP {

// -----
// Absolute
// -----

Sum::Sum(bool ignore_missing, std::shared_ptr<Node> next)
    : next_(next)
    , ignore_missing_(ignore_missing)
{
}

Sum::Sum(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    ignore_missing_ = ptree.get<bool>("ignore_missing");
}

void Sum::complete() {
    next_->complete();
}

bool Sum::put(const aku_Sample& sample) {
    MutableSample mut(&sample);
    auto size = mut.size();
    double acc = 0.;
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        double x;
        if (value == nullptr) {
            if (ignore_missing_) {
                x = 0.0;
            } else {
                x = std::numeric_limits<double>::quiet_NaN();
            }
        } else {
            x = *value;
        }
        acc += x;
    }
    mut.collapse();
    double* value = mut[0];
    *value = acc;
    return mut.publish(next_.get());
}

void Sum::set_error(aku_Status status) {
    next_->set_error(status);
}

int Sum::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<Sum> token("sum");

}}  // namespace
