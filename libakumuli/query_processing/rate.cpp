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

bool SimpleRate::put(MutableSample &mut) {
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(mut.get_paramid(), ix);
            double oldX = 0;
            aku_Timestamp oldT = 0;
            auto it = table_.find(key);
            if (it != table_.end()) {
                oldT = std::get<0>(it->second);
                oldX = std::get<1>(it->second);
            }
            auto newT = mut.get_timestamp();
            double newX = *value;
            // Formula: rate = Δx/Δt
            const double nsec = 1000000000;
            double dX = (newX - oldX) / (newT - oldT) * nsec;
            *value = dX;
            table_[key] = std::make_tuple(newT, newX);
        }
    }
    return next_->put(mut);
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

CumulativeSum::CumulativeSum(std::shared_ptr<Node> next)
    : next_(next)
{
}

CumulativeSum::CumulativeSum(const boost::property_tree::ptree&, std::shared_ptr<Node> next)
    : next_(next)
{
}

void CumulativeSum::complete() {
    next_->complete();
}

bool CumulativeSum::put(MutableSample &mut) {
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(mut.get_paramid(), ix);
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
    return next_->put(mut);
}

void CumulativeSum::set_error(aku_Status status) {
    next_->set_error(status);
}

int CumulativeSum::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<SimpleRate> rate_token("rate");

static QueryParserToken<CumulativeSum>  sum_token("accumulate");
static QueryParserToken<CumulativeSum>  cusum_token("cusum");

}}  // namespace

