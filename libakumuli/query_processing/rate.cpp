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

static double get_first_value(const aku_Sample* sample) {
    union {
        double d;
        u64 u;
    } bits;
    bits.d = sample->payload.float64;
    assert(sample->payload.type == AKU_PAYLOAD_TUPLE && bits.u == 0x400000000000001ul);
    const double* value = reinterpret_cast<const double*>(sample->payload.data);
    return *value;
}

static void set_first_value(aku_Sample* sample, double x) {
    union {
        double d;
        u64 u;
    } bits;
    bits.d = sample->payload.float64;
    assert(sample->payload.type == AKU_PAYLOAD_TUPLE && bits.u == 0x400000000000001ul);
    double* value = reinterpret_cast<double*>(sample->payload.data);
    *value = x;
}

bool SimpleRate::put(const aku_Sample& sample) {
    // Formula: rate = Δx/Δt
    bool tuple = false;
    if (sample.payload.type != AKU_PAYLOAD_FLOAT) {
        if (sample.payload.type == AKU_PAYLOAD_TUPLE) {
            union {
                double d;
                u64 u;
            } bits;
            bits.d = sample.payload.float64;
            if (bits.u != 0x400000000000001ul) {
                // only one element tuples supported
                return false;
            }
            tuple = true;
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
        auto newX = get_first_value(&sample);
        auto newT = sample.timestamp;

        char buffer[sizeof(aku_Sample) + sizeof(double)];
        aku_Sample *result = reinterpret_cast<aku_Sample*>(buffer);
        memcpy(result, &sample, sample.payload.size == 0 ? sizeof(aku_Sample) : sample.payload.size);
        const double nsec = 1000000000;
        double x = (newX - oldX) / (newT - oldT) * nsec;
        set_first_value(result, x);

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

