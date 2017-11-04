#pragma once
#include "akumuli_def.h"

// C++ headers
#include <cinttypes>
#include <deque>
#include <math.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace Akumuli {
namespace QP {

/** Anomaly detector interface */
struct AnomalyDetectorIface {
    virtual ~AnomalyDetectorIface() = default;
    virtual void add(u64 id, double value) = 0;
    virtual bool is_anomaly_candidate(u64 id) const = 0;
    virtual void move_sliding_window()              = 0;
};

struct AnomalyDetectorUtil {

    //! Create approximate anomaly detector based on simple moving-average smothing
    static std::unique_ptr<AnomalyDetectorIface> create_approx_sma(u32 N, u32 K, double threshold,
                                                                   u32 window_size);

    //! Create precise anomaly detector based on simple moving-average smothing
    static std::unique_ptr<AnomalyDetectorIface> create_precise_sma(double threshold,
                                                                    u32    window_size);

    //! Create approximate anomaly detector based on EWMA smoothing
    static std::unique_ptr<AnomalyDetectorIface> create_approx_ewma(u32 N, u32 K, double threshold,
                                                                    double alpha);

    //! Create precise anomaly detector based on EWMA smoothing
    static std::unique_ptr<AnomalyDetectorIface> create_precise_ewma(double threshold,
                                                                     double alpha);

    //! Create precise anomaly detector based on double Holt-Winters smoothing
    static std::unique_ptr<AnomalyDetectorIface>
    create_precise_double_exp_smoothing(double threshold, double alpha, double beta);

    //! Create approximate anomaly detector based on double Holt-Winters smoothing
    static std::unique_ptr<AnomalyDetectorIface>
    create_approx_double_exp_smoothing(u32 N, u32 K, double threshold, double alpha, double beta);

    static std::unique_ptr<AnomalyDetectorIface>
    create_precise_holt_winters(double threshold, double alpha, double beta, double gamma,
                                int period);

    static std::unique_ptr<AnomalyDetectorIface>
    create_approx_holt_winters(u32 N, u32 K, double threshold, double alpha, double beta,
                               double gamma, int period);
};

}
}
