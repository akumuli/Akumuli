#pragma once

#include <memory>

#include "queryprocessor_fwd.h"
#include "anomalydetector.h"

namespace Akumuli {
namespace QP {


struct AnomalyDetector : Node {
    typedef std::unique_ptr<AnomalyDetectorIface> PDetector;

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

    std::shared_ptr<Node> next_;
    PDetector detector_;

    AnomalyDetector(uint32_t nhashes,
                    uint32_t bits,
                    double   threshold,
                    double   alpha,
                    double   beta,
                    double   gamma,
                    int      period,
                    FcastMethod method,
                    std::shared_ptr<Node> next);

    virtual void complete();

    virtual bool put(const aku_Sample &sample);

    virtual void set_error(aku_Status status);

    virtual NodeType get_type() const;
};


}}  // namespace
