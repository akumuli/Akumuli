#include "rate.h"
#include "../storage_engine/operators/aggregate.h"

namespace Akumuli {
namespace QP {

// ----------
// SimpleRate
// ----------

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
    MutableSample mut(&sample);
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(sample.paramid, ix);
            double oldX = 0;
            aku_Timestamp oldT = 0;
            auto it = table_.find(key);
            if (it != table_.end()) {
                oldT = std::get<0>(it->second);
                oldX = std::get<1>(it->second);
            }
            auto newT = sample.timestamp;
            double newX = *value;
            // Formula: rate = Δx/Δt
            const double nsec = 1000000000;
            double dX = (newX - oldX) / (newT - oldT) * nsec;
            *value = dX;
        }
    }
    return mut.publish(next_.get());
}

void SimpleRate::set_error(aku_Status status) {
    next_->set_error(status);
}

int SimpleRate::get_requirements() const {
    return TERMINAL;
}

// ---------
// SimpleSum
// ---------

SimpleSum::SimpleSum(std::shared_ptr<Node> next)
    : next_(next)
{
}

SimpleSum::SimpleSum(const boost::property_tree::ptree&, std::shared_ptr<Node> next)
    : next_(next)
{
}

void SimpleSum::complete() {
    next_->complete();
}

bool SimpleSum::put(const aku_Sample& sample) {
    MutableSample mut(&sample);
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(sample.paramid, ix);
            double prev = 0;
            auto it = table_.find(key);
            if (it != table_.end()) {
                prev = it->second;
            } else {
                table_[key] = 0;
                it = table_.find(key);
            }
            double cur = *value;
            double sum = cur + prev;
            *value = sum;
            it->second = sum;
        }
    }
    return mut.publish(next_.get());
}

void SimpleSum::set_error(aku_Status status) {
    next_->set_error(status);
}

int SimpleSum::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<SimpleRate> rate_token("rate");

static QueryParserToken<SimpleSum>  sum_token("sum");

}}  // namespace

