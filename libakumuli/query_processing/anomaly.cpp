#include "anomaly.h"
#include "../util.h"
#include "../storage_engine/operators/aggregate.h"

#include <boost/exception/all.hpp>

namespace Akumuli {
namespace QP {

static Forecasting::FcastMethod parse_anomaly_detector_type(boost::property_tree::ptree const& ptree) {
    bool approx = ptree.get<bool>("approx");
    std::string name = ptree.get<std::string>("method");
    Forecasting::FcastMethod method;
    if (name == "ewma" || name == "exp-smoothing") {
        method = approx ? Forecasting::EWMA_SKETCH : Forecasting::EWMA;
    } else if (name == "sma" || name == "simple-moving-average") {
        method = approx ? Forecasting::SMA_SKETCH : Forecasting::SMA;
    } else if (name == "double-exp-smoothing") {
        method = approx ? Forecasting::DOUBLE_EXP_SMOOTHING_SKETCH : Forecasting::DOUBLE_EXP_SMOOTHING;
    } else if (name == "holt-winters") {
        method = approx ? Forecasting::HOLT_WINTERS_SKETCH : Forecasting::HOLT_WINTERS;
    } else {
        QueryParserError err("Unknown forecasting method");
        BOOST_THROW_EXCEPTION(err);
    }
    return method;
}

static void validate_sketch_params(boost::property_tree::ptree const& ptree) {
    u32 bits = ptree.get<u32>("bits", 8);
    u32 hashes = ptree.get<u32>("hashes", 1);
    // bits should be in range
    if (bits < 8 || bits > 16) {
        QueryParserError err("Anomaly detector parameter `bits` out of range");
        BOOST_THROW_EXCEPTION(err);
    }
    // hashes should be in range and odd
    if (hashes % 2 == 0) {
        QueryParserError err("Anomaly detector parameter `hashes` should be odd");
        BOOST_THROW_EXCEPTION(err);
    }
    if (hashes == 0 || hashes > 9) {
        QueryParserError err("Anomaly detector parameter `hashes` out of range");
        BOOST_THROW_EXCEPTION(err);
    }
}

static void validate_all_params(std::vector<std::string> required, boost::property_tree::ptree const& ptree) {
    for (auto name: required) {
        auto o = ptree.get_optional<std::string>(name);
        if (!o) {
            std::string err_msg = "Parameter " + name + " should be set";
            QueryParserError err(err_msg.c_str());
            BOOST_THROW_EXCEPTION(err);
        }
    }
}

static void validate_anomaly_detector_params(boost::property_tree::ptree const& ptree) {
    auto type = parse_anomaly_detector_type(ptree);
    switch(type) {
    case Forecasting::SMA_SKETCH:
        validate_sketch_params(ptree);
    case Forecasting::SMA:
        validate_all_params({"period"}, ptree);
        break;

    case Forecasting::EWMA_SKETCH:
        validate_sketch_params(ptree);
    case Forecasting::EWMA:
        validate_all_params({"alpha"}, ptree);
        break;

    case Forecasting::DOUBLE_EXP_SMOOTHING_SKETCH:
        validate_sketch_params(ptree);
    case Forecasting::DOUBLE_EXP_SMOOTHING:
        validate_all_params({"alpha", "gamma"}, ptree);
        break;

    case Forecasting::HOLT_WINTERS_SKETCH:
        validate_sketch_params(ptree);
    case Forecasting::HOLT_WINTERS:
        validate_all_params({"alpha", "beta", "gamma", "period"}, ptree);
        break;
    }
}

static void validate_coef(double value, double range_begin, double range_end, const char* err_msg) {
    if (value >= range_begin && value <= range_end) {
        return;
    }
    QueryParserError err(err_msg);
    BOOST_THROW_EXCEPTION(err);
}

AnomalyDetector::AnomalyDetector(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next)
    : next_(next)
{
    validate_anomaly_detector_params(ptree);
    double threshold = ptree.get<double>("threshold");
    u32 bits = ptree.get<u32>("bits", 10u);
    u32 nhashes = ptree.get<u32>("hashes", 3u);
    AnomalyDetector::FcastMethod method = parse_anomaly_detector_type(ptree);
    double alpha = ptree.get<double>("alpha", 0.0);
    double beta = ptree.get<double>("beta", 0.0);
    double gamma = ptree.get<double>("gamma", 0.0);
    int period = ptree.get<int>("period", 0);
    validate_coef(alpha, 0.0, 1.0, "`alpha` should be in [0, 1] range");
    validate_coef(beta,  0.0, 1.0, "`beta` should be in [0, 1] range");
    validate_coef(gamma, 0.0, 1.0, "`gamma` should be in [0, 1] range");

    switch(method) {
    case Forecasting::SMA:
        detector_ = AnomalyDetectorUtil::create_precise_sma(threshold, period);
        break;
    case Forecasting::SMA_SKETCH:
        detector_ = AnomalyDetectorUtil::create_approx_sma(nhashes, 1 << bits, threshold, period);
        break;
    case Forecasting::EWMA:
        detector_ = AnomalyDetectorUtil::create_precise_ewma(threshold, alpha);
        break;
    case Forecasting::EWMA_SKETCH:
        detector_ = AnomalyDetectorUtil::create_approx_ewma(nhashes, 1 << bits, threshold, alpha);
        break;
    case Forecasting::DOUBLE_EXP_SMOOTHING:
        detector_ = AnomalyDetectorUtil::create_precise_double_exp_smoothing(threshold, alpha, gamma);
        break;
    case Forecasting::DOUBLE_EXP_SMOOTHING_SKETCH:
        detector_ = AnomalyDetectorUtil::create_approx_double_exp_smoothing(nhashes, 1 << bits, threshold, alpha, gamma);
        break;
    case Forecasting::HOLT_WINTERS:
        detector_ = AnomalyDetectorUtil::create_precise_holt_winters(threshold, alpha, beta, gamma, period);
        break;
    case Forecasting::HOLT_WINTERS_SKETCH:
        detector_ = AnomalyDetectorUtil::create_approx_holt_winters(nhashes, 1 << bits, threshold, alpha, beta, gamma, period);
        break;
    default:
        AKU_PANIC("AnomalyDetector building error");
    }
}

AnomalyDetector::AnomalyDetector(
        u32 nhashes,
        u32 bits,
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
        case Forecasting::SMA:
            detector_ = AnomalyDetectorUtil::create_precise_sma(threshold, period);
            break;
        case Forecasting::SMA_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_sma(nhashes, 1 << bits, threshold, period);
            break;
        case Forecasting::EWMA:
            detector_ = AnomalyDetectorUtil::create_precise_ewma(threshold, alpha);
            break;
        case Forecasting::EWMA_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_ewma(nhashes, 1 << bits, threshold, alpha);
            break;
        case Forecasting::DOUBLE_EXP_SMOOTHING:
            detector_ = AnomalyDetectorUtil::create_precise_double_exp_smoothing(threshold, alpha, gamma);
            break;
        case Forecasting::DOUBLE_EXP_SMOOTHING_SKETCH:
            detector_ = AnomalyDetectorUtil::create_approx_double_exp_smoothing(nhashes, 1 << bits, threshold, alpha, gamma);
            break;
        case Forecasting::HOLT_WINTERS:
            detector_ = AnomalyDetectorUtil::create_precise_holt_winters(threshold, alpha, beta, gamma, period);
            break;
        case Forecasting::HOLT_WINTERS_SKETCH:
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
    return false;
}

void AnomalyDetector::set_error(aku_Status status) {
    next_->set_error(status);
}

int AnomalyDetector::get_requirements() const {
    return TERMINAL|GROUP_BY_REQUIRED;
}


}}  // namespace

