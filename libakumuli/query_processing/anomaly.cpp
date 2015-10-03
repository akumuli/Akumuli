#include "anomaly.h"

namespace Akumuli {
namespace QP {

AnomalyDetector::AnomalyDetector(
        uint32_t nhashes,
        uint32_t bits,
        double   threshold,
        double   alpha,
        double   beta,
        double   gamma,
        int      period,
        FcastMethod method,
        std::shared_ptr<Node> next)
    : next_(next)
{
    try {
        switch(method) {
        case SMA:
            detector_ = AnomalyDetectorUtil::create_precise_sma(threshold, period);
            break;
        case SMA_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_sma(nhashes, 1 << bits, threshold, period);
            break;
        case EWMA:
            detector_ = AnomalyDetectorUtil::create_precise_ewma(threshold, alpha);
            break;
        case EWMA_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_ewma(nhashes, 1 << bits, threshold, alpha);
            break;
        case DOUBLE_EXP_SMOOTHING:
            detector_ = AnomalyDetectorUtil::create_precise_double_exp_smoothing(threshold, alpha, gamma);
            break;
        case DOUBLE_EXP_SMOOTHING_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_double_exp_smoothing(nhashes, 1 << bits, threshold, alpha, gamma);
            break;
        case HOLT_WINTERS:
            detector_ = AnomalyDetectorUtil::create_precise_holt_winters(threshold, alpha, beta, gamma, period);
            break;
        case HOLT_WINTERS_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_holt_winters(nhashes, 1 << bits, threshold, alpha, beta, gamma, period);
            break;
        default:
            std::logic_error err("AnomalyDetector building error");  // invalid use of the constructor
            BOOST_THROW_EXCEPTION(err);
        }
    } catch (...) {
        // std::cout << boost::current_exception_diagnostic_information() << std::endl;
        throw;
    }
}

void AnomalyDetector::complete() {
    next_->complete();
}

bool AnomalyDetector::put(const aku_Sample &sample) {
    if (sample.payload.type == aku_PData::EMPTY) {
        detector_->move_sliding_window();
        return next_->put(sample);
    } else if (sample.payload.type & aku_PData::FLOAT_BIT) {
        detector_->add(sample.paramid, sample.payload.float64);
        if (detector_->is_anomaly_candidate(sample.paramid)) {
            aku_Sample anomaly = sample;
            anomaly.payload.type |= aku_PData::URGENT;
            return next_->put(anomaly);
        }
    }
    // Ignore BLOBs
    return true;
}

void AnomalyDetector::set_error(aku_Status status) {
    next_->set_error(status);
}

Node::NodeType AnomalyDetector::get_type() const {
    return Node::AnomalyDetector;
}

}}  // namespace

