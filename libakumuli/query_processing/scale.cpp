#include "scale.h"

namespace Akumuli {
namespace QP {

// -----
// Scale
// -----

Scale::Scale(std::vector<double> weights, std::shared_ptr<Node> next)
    : weights_(weights)
    , next_(next)
{
}

Scale::Scale(const boost::property_tree::ptree& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    // Read weights from json
    auto const& list = ptree.get_child_optional("weights");
    if (list) {
        for (const auto& value: *list) {
            double val = value.second.get_value<double>();
            weights_.push_back(val);
        }
    }
}

void Scale::complete() {
    next_->complete();
}

bool Scale::put(MutableSample &mut) {
    auto size = std::min(mut.size(), static_cast<u32>(weights_.size()));
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        double weight = weights_[ix];
        if (value) {
            *value *= weight;
        }
    }
    return next_->put(mut);
}

void Scale::set_error(aku_Status status) {
    next_->set_error(status);
}

int Scale::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<Scale> scale_token("scale");

}}  // namespace
