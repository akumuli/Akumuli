#include "scale.h"

namespace Akumuli {
namespace QP {

// ---------
// SimpleSum
// ---------

Scale::Scale(std::vector<double> weights, std::shared_ptr<Node> next)
    : weights_(weights)
    , next_(next)
{
}

Scale::Scale(const boost::property_tree::ptree&, std::shared_ptr<Node> next)
    : next_(next)
{
}

void Scale::complete() {
    next_->complete();
}

bool Scale::put(const aku_Sample& sample) {
    MutableSample mut(&sample);
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        double weight = weights_[ix];
        if (value) {
            *value *= weight;
        }
    }
    return mut.publish(next_.get());
}

void Scale::set_error(aku_Status status) {
    next_->set_error(status);
}

int Scale::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<Scale> scale_token("scale");

}}  // namespace
