#pragma once
#include <cinttypes>
#include <vector>
#include <deque>
#include <unordered_map>
#include <memory>
#include <math.h>

namespace Akumuli {
namespace QP {

/** Anomaly detector interface */
struct AnomalyDetectorIface {
    virtual void add(uint64_t id, double value) = 0;
    virtual bool is_anomaly_candidate(uint64_t id) const = 0;
    virtual void move_sliding_window() = 0;
};

struct AnomalyDetectorUtil {

    //! Create approximate anomaly detector based on simple moving-average smothing
    static std::unique_ptr<AnomalyDetectorIface>
                          create_approx_sma (uint32_t N,
                                             uint32_t K,
                                             double threshold,
                                             uint32_t window_size);

    //! Create precise anomaly detector based on simple moving-average smothing
    static std::unique_ptr<AnomalyDetectorIface>
                         create_precise_sma (double threshold,
                                             uint32_t window_size);

    //! Create approximate anomaly detector based on EWMA smoothing
    static std::unique_ptr<AnomalyDetectorIface>
                         create_approx_ewma (uint32_t N,
                                             uint32_t K,
                                             double threshold,
                                             double alpha);

    //! Create precise anomaly detector based on EWMA smoothing
    static std::unique_ptr<AnomalyDetectorIface>
                         create_precise_ewma(double threshold,
                                             double alpha);

    //! Create precise anomaly detector based on double Holt-Winters smoothing
    static std::unique_ptr<AnomalyDetectorIface>
                         create_precise_double_exp_smoothing(
                                             double threshold,
                                             double alpha,
                                             double beta);

    //! Create approximate anomaly detector based on double Holt-Winters smoothing
    static std::unique_ptr<AnomalyDetectorIface>
                         create_approx_double_exp_smoothing(
                                             uint32_t N,
                                             uint32_t K,
                                             double threshold,
                                             double alpha,
                                             double beta);

    static std::unique_ptr<AnomalyDetectorIface>
                         create_precise_holt_winters(
                                             double threshold,
                                             double alpha,
                                             double beta,
                                             double gamma,
                                             int period);

    static std::unique_ptr<AnomalyDetectorIface>
                         create_approx_holt_winters(
                                             uint32_t N,
                                             uint32_t K,
                                             double threshold,
                                             double alpha,
                                             double beta,
                                             double gamma,
                                             int period);
};

}
}
