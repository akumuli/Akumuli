#include "sliding_window.h"
#include "../storage_engine/operators/aggregate.h"

namespace Akumuli {
namespace QP {

// -------------
// SlidingWindow
// -------------

// ----
// EWMA
// ----

static const u32 EWMA_WARMUP = 10;

EWMA::EWMA()
    : warmup_(0)
    , value_(0.0)
    , decay_(0.0)
{
}

EWMA::EWMA(double decay)
    : warmup_(0)
    , value_(0.0)
    , last_(0)
    , decay_(decay)
{
}

void EWMA::add(double value) {
    if (warmup_ < EWMA_WARMUP) {
        value_ += value;
        warmup_++;
    } else if (warmup_ == EWMA_WARMUP) {
        warmup_++;
        value_ += value;
        value_ = value_ / static_cast<double>(warmup_);
        value_ = (value * decay_) + (value_ * (1.0 - decay_));
    } else {
        value_ = (value * decay_) + (value_ * (1.0 - decay_));
    }
}


double EWMA::get(double default_value) const {
    if (warmup_ <= EWMA_WARMUP) {
        return default_value;
    }
    return value_;
}

// -------------
// EWMAPrecition
// -------------

EWMAPrediction::EWMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : next_(next)
    , delta_(false)
{
    decay_ = ptree.get<double>("decay");
}

EWMAPrediction::EWMAPrediction(double decay, bool calculate_delta, std::shared_ptr<Node> next)
    : decay_(decay)
    , next_(next)
    , delta_(calculate_delta)
{
}

void EWMAPrediction::complete() {
    next_->complete();
}

bool EWMAPrediction::put(MutableSample &mut) {
    if ((mut.payload_.sample.payload.type & aku_PData::REGULLAR) == 0) {
        // Not supported, query require regullar data
        set_error(AKU_EREGULLAR_EXPECTED);
        return false;
    }
    auto size = mut.size();

    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(mut.get_paramid(), ix);
            if (swind_.count(key) == 0) {
                swind_[key] = EWMA(decay_);
            }
            EWMA& ewma = swind_[key];
            double exp = ewma.get(*value);
            ewma.add(*value);
            if (delta_) {
                *value -= exp;
            } else {
                *value  = exp;
            }
        }
    }

    return next_->put(mut);
}

void EWMAPrediction::set_error(aku_Status status) {
    next_->set_error(status);
}

int EWMAPrediction::get_requirements() const {
    return TERMINAL;
}

struct EWMAPredictionError : EWMAPrediction {
    EWMAPredictionError(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
        : EWMAPrediction(ptree.get<double>("decay"), true, next)
    {
    }
};


// -------------------
// SimpleMovingAverage
// -------------------

SMA::SMA()
    : buffer_(1)
    , sum_(0)
{
}

SMA::SMA(size_t n)
    : buffer_(n)
    , sum_(0)
{
}


void SMA::add(double value) {
    // remove old element
    if (!buffer_.empty()) {
        double old = buffer_.front();
        buffer_.pop_front();
        sum_ -= old;
    }
    buffer_.push_back(value);
    sum_ += value;
}

double SMA::get() const {
    return sum_ / buffer_.size();
}

SMAPrediction::SMAPrediction(size_t window_width, bool calculate_delta, std::shared_ptr<Node> next)
    : width_(window_width)
    , delta_(calculate_delta)
{
}

SMAPrediction::SMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : delta_(false)
{
    width_ = ptree.get<double>("window-width");
}

void SMAPrediction::complete() {
    next_->complete();
}

bool SMAPrediction::put(MutableSample& mut) {
    if ((mut.payload_.sample.payload.type & aku_PData::REGULLAR) == 0) {
        // Not supported, query require regullar data
        set_error(AKU_EREGULLAR_EXPECTED);
        return false;
    }
    auto size = mut.size();
    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(mut.get_paramid(), ix);
            if (swind_.count(key) == 0) {
                swind_[key] = SMA(width_);
            }
            SMA& sma = swind_[key];
            double exp = sma.get();
            sma.add(*value);
            if (delta_) {
                *value -= exp;
            } else {
                *value  = exp;
            }
        }
    }

    return next_->put(mut);
}

void SMAPrediction::set_error(aku_Status status) {
    next_->set_error(status);
}

int SMAPrediction::get_requirements() const {
    return TERMINAL;
}


struct SMAPredictionError : SMAPrediction {
    SMAPredictionError(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
        : SMAPrediction(ptree.get<size_t>("window-width"), true, next)
    {
    }
};

// -------------------------
// Cumulative moving average
// -------------------------

CMAPrediction::CMAPrediction(std::shared_ptr<Node> next)
    : next_(next)
{
}

CMAPrediction::CMAPrediction(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
}

void CMAPrediction::complete() {
    next_->complete();
}

bool CMAPrediction::put(MutableSample& mut) {
    auto size = mut.size();

    for (u32 ix = 0; ix < size; ix++) {
        double* value = mut[ix];
        if (value) {
            // calculate new value
            auto key = std::make_tuple(mut.get_paramid(), ix);
            double sum;
            size_t cnt;
            std::tie(sum, cnt) = swind_[key];
            sum += *value;
            cnt += 1;
            swind_[key] = std::make_pair(sum + *value, cnt + 1);
            *value = sum / cnt;
        }
    }
    return next_->put(mut);
}

void CMAPrediction::set_error(aku_Status status) {
    next_->set_error(status);
}

int CMAPrediction::get_requirements() const {
    return TERMINAL;
}

static QueryParserToken<EWMAPredictionError> ewma_error_token("ewma-error");
static QueryParserToken<EWMAPrediction> ewma_token("ewma");

static QueryParserToken<SMAPredictionError> sma_error_token("sma-error");
static QueryParserToken<SMAPrediction> sma_token("sma");

static QueryParserToken<CMAPrediction> cma_token("cma");

}}  // namespace
