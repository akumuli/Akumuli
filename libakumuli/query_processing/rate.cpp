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
    aku_Timestamp oldT = 0;
    double oldX = 0;
    aku_ParamId id = sample.paramid;
    auto it = table_.find(id);
    if (it != table_.end()) {
        oldT = std::get<0>(it->second);
        oldX = std::get<1>(it->second);
    }
    auto newT = sample.timestamp;
    double newX;
    SampleUtil::Context ctx;
    std::tie(newX, ctx) = SampleUtil::get_value(sample);
    if (ctx == SampleUtil::ERROR) {
        return false;
    }
    // Formula: rate = Δx/Δt
    const double nsec = 1000000000;
    double dX = (newX - oldX) / (newT - oldT) * nsec;
    // publish next value
    return SampleUtil::publish(ctx, dX, sample, next_.get());
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
    double prev = 0;
    aku_ParamId id = sample.paramid;
    auto it = table_.find(id);
    if (it != table_.end()) {
        prev = it->second;
    }
    double curr;
    SampleUtil::Context ctx;
    std::tie(curr, ctx) = SampleUtil::get_value(sample);
    if (ctx == SampleUtil::ERROR) {
        return false;
    }
    double sum = curr + prev;
    // publish next value
    return SampleUtil::publish(ctx, sum, sample, next_.get());
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

