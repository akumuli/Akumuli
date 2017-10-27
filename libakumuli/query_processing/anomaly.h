#pragma once

#include <memory>

#include "../anomalydetector.h"
#include "../queryprocessor_framework.h"

namespace Akumuli {
namespace QP {

namespace Forecasting {
    enum FcastMethod {
        SMA,
        SMA_SKETCH,
        EWMA,
        EWMA_SKETCH,
        DOUBLE_EXP_SMOOTHING,
        DOUBLE_EXP_SMOOTHING_SKETCH,
        HOLT_WINTERS,
        HOLT_WINTERS_SKETCH,
    };
}

struct AnomalyDetector : Node {
    typedef std::unique_ptr<AnomalyDetectorIface> PDetector;
    typedef Forecasting::FcastMethod FcastMethod;

    std::shared_ptr<Node> next_;
    PDetector             detector_;

    AnomalyDetector(u32 nhashes, u32 bits, double threshold, double alpha, double beta,
                    double gamma, int period, FcastMethod method, std::shared_ptr<Node> next);

    AnomalyDetector(boost::property_tree::ptree const& ptree, std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(const aku_Sample& sample);

    virtual void set_error(aku_Status status);

    virtual int get_requirements() const;
};

}
}  // namespace
