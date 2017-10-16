#include "rate.h"

namespace Akumuli {
namespace QP {

SimpleRate::SimpleRate(std::shared_ptr<Node> next)
    : next_(next)
{
}

SimpleRate::SimpleRate(const boost::property_tree::ptree&, std::shared_ptr<Node> next)
    : next_(next)
{
}

void SimpleRate::complete() {
    next_->complete();
}

bool SimpleRate::put(const aku_Sample& sample) {
    if (sample.payload.type != AKU_PAYLOAD_FLOAT) {
        // If margin or empty - continue
        return false;
    }
    // Formula: rate = Δx/Δt
    aku_Timestamp oldT = 0;
    double oldX = 0;

    aku_ParamId id = sample.paramid;
    auto it = table_.find(id);
    if (it != table_.end()) {
        oldT = std::get<0>(it->second);
        oldX = std::get<1>(it->second);
    }
    auto newX = sample.payload.float64;
    auto newT = sample.timestamp;

    aku_Sample result = sample;
    const double nsec = 1000000000;
    result.payload.float64 = (newX - oldX) / (newT - oldT) * nsec;

    // Update table
    if (it != table_.end()) {
        it->second = std::make_tuple(newT, newX);
    } else {
        table_[id] = std::make_tuple(newT, newX);
    }

    return next_->put(result);
}

void SimpleRate::set_error(aku_Status status) {
    next_->set_error(status);
}

int SimpleRate::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<SimpleRate> rate_token("rate");

}}  // namespace

