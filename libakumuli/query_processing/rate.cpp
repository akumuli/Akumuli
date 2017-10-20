#include "rate.h"
#include "../storage_engine/operators/aggregate.h"

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
    // Formula: rate = Δx/Δt
    bool tuple = false;
    if (sample.payload.type != AKU_PAYLOAD_FLOAT) {
        if (TupleOutputUtils::is_one_element_tuple(&sample)) {
            tuple = true;
        } else {
            return false;
        }
    }
    if (!tuple) {
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
        double x = (newX - oldX) / (newT - oldT) * nsec;
        result.payload.float64 = x;

        // Update table
        if (it != table_.end()) {
            it->second = std::make_tuple(newT, newX);
        } else {
            table_[id] = std::make_tuple(newT, newX);
        }
        return next_->put(result);

    } else {
        aku_Timestamp oldT = 0;
        double oldX = 0;

        aku_ParamId id = sample.paramid;
        auto it = table_.find(id);
        if (it != table_.end()) {
            oldT = std::get<0>(it->second);
            oldX = std::get<1>(it->second);
        }
        auto newX = TupleOutputUtils::get_first_value(&sample);
        auto newT = sample.timestamp;

        const size_t buffer_size = sizeof(aku_Sample) + sizeof(double);
        char buffer[buffer_size];
        aku_Sample *result = TupleOutputUtils::copy_sample(&sample, buffer, buffer_size);
        const double nsec = 1000000000;
        double x = (newX - oldX) / (newT - oldT) * nsec;
        TupleOutputUtils::set_first_value(result, x);

        // Update table
        if (it != table_.end()) {
            it->second = std::make_tuple(newT, newX);
        } else {
            table_[id] = std::make_tuple(newT, newX);
        }

        return next_->put(*result);
    }
}

void SimpleRate::set_error(aku_Status status) {
    next_->set_error(status);
}

int SimpleRate::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<SimpleRate> rate_token("rate");

}}  // namespace

