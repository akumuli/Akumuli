#include "akumuli_def.h"
#include "anomalydetector.h"
#include "hashfnfamily.h"
#include "queryprocessor_framework.h"

#include <random>
#include <stdexcept>

#include <boost/exception/all.hpp>

namespace Akumuli {
namespace QP {

//                          //
//      CountingSketch      //
//                          //

struct CountingSketch {
    HashFnFamily const& hashes_;
    const u32 N;
    const u32 K;
    double sum_;
    std::vector<std::vector<double>> tables_;

    CountingSketch(HashFnFamily const& hf)
        : hashes_(hf)
        , N(hf.N)
        , K(hf.K)
        , sum_(0.0)
    {
        for (u32 i = 0u; i < N; i++) {
            std::vector<double> row;
            row.resize(K, 0.0);
            tables_.push_back(std::move(row));
        }
    }

    CountingSketch(CountingSketch const& cs)
        : hashes_(cs.hashes_)
        , N(cs.N)
        , K(cs.K)
        , sum_(cs.sum_)
    {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double> row;
            row.resize(K, 0.0);
            std::vector<double> const& rcs = cs.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = rcs[col];
            }
            tables_.push_back(std::move(row));
        }
    }

    void _update_sum() {
        sum_ = 0.0;
        for (auto val: tables_[0]) {
            sum_ += val;
        }
    }

    void add(u64 id, double value) {
        sum_ += value;
        for (u32 i = 0; i < N; i++) {
            // calculate hash from id to K
            u32 hash = hashes_.hash(i, id);
            tables_[i][hash] += value;
        }
    }

    //! Second moment estimator
    double estimateF2() const {
        std::vector<double> results;
        auto f = 1./(K - 1);
        for (u32 i = 0u; i < N; i++) {
            double rowsum = std::accumulate(tables_[i].begin(), tables_[i].end(), 0.0, [](double acc, double val) {
                return acc + val*val;
            });
            double res = K*f*sqrt(rowsum) - f*sum_*sum_;
            results.push_back(res);
        }
        std::sort(results.begin(), results.end());
        return results[N/2];
    }

    //! Unbiased value estimator
    double estimate(u64 id) const {
        std::vector<double> results;
        for (u32 i = 0u; i < N; i++) {
            u32 hash = hashes_.hash(i, id);
            double value = tables_[i][hash];
            double estimate = (value - sum_/K)/(1. - 1./K);
            results.push_back(estimate);
        }
        std::sort(results.begin(), results.end());
        return results[N/2];
    }

    //! current sketch <- absolute difference between two arguments
    void diff(CountingSketch const& lhs, CountingSketch const& rhs) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            std::vector<double> const& lrow = lhs.tables_[ixrow];
            std::vector<double> const& rrow = rhs.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = std::fabs(lrow[col] - rrow[col]);
            }
        }
        _update_sum();
    }

    //! Add sketch
    void add(CountingSketch const& val) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            std::vector<double> const& rval = val.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = row[col] + rval[col];
            }
        }
        _update_sum();
    }

    //! Substract sketch
    void sub(CountingSketch const& val) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            std::vector<double> const& rval = val.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = row[col] - rval[col];
            }
        }
        _update_sum();
    }

    //! Multiply sketch by value
    void mul(double value) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] *= value;
            }
        }
        _update_sum();
    }

    //! Multiply by another sketch
    void mul(CountingSketch const& value) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            std::vector<double> const& rval = value.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = row[col] * rval[col];
            }
        }
        _update_sum();
    }

    //! Divide by another sketch
    void div(CountingSketch const& value) {
        for (auto ixrow = 0u; ixrow < N; ixrow++) {
            std::vector<double>& row = tables_[ixrow];
            std::vector<double> const& rval = value.tables_[ixrow];
            for (auto col = 0u; col < K; col++) {
                row[col] = row[col] / rval[col];
            }
        }
        _update_sum();
    }
};


//                          //
//      PreciseCounter      //
//                          //

struct PreciseCounter {
    std::unordered_map<u64, double> table_;

    //! C-tor. Parameter `hf` is unused for the sake of interface unification.
    PreciseCounter(HashFnFamily const& hf) {
    }

    PreciseCounter(PreciseCounter const& cs)
        : table_(cs.table_)
    {
    }

    void add(u64 id, double value) {
        table_[id] += value;
    }

    //! Unbiased value estimator
    double estimate(u64 id) const {
        auto it = table_.find(id);
        if (it != table_.end()) {
            return it->second;
        }
        return 0.;
    }

    //! Second moment estimator
    double estimateF2() const {
        double sum = std::accumulate(table_.begin(), table_.end(), 0.0,
                                     [](double acc, std::pair<u64, double> pval) {
            return acc + pval.second*pval.second;
        });
        return sqrt(sum);
    }

    //! current sketch <- absolute difference between two arguments
    void diff(PreciseCounter const& lhs, PreciseCounter const& rhs) {
        const std::unordered_map<u64, double> *small, *large;
        if (lhs.table_.size() < rhs.table_.size()) {
            small = &lhs.table_;
            large = &rhs.table_;
        } else {
            small = &rhs.table_;
            large = &lhs.table_;
        }
        table_.clear();
        // Scan largest
        for (auto it = large->begin(); it != large->end(); it++) {
            auto small_it = small->find(it->first);
            double val = 0.;
            if (small_it != small->end()) {
                val = small_it->second;
            }
            table_[it->first] = std::fabs(it->second - val);
        }
    }

    //! Add sketch
    void add(PreciseCounter const& val) {
        for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
            table_[it->first] += it->second;
        }
    }

    //! Substract sketch
    void sub(PreciseCounter const& val) {
        for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
            table_[it->first] -= it->second;
        }
    }

    //! Multiply sketch by value
    void mul(double value) {
        for(auto it = table_.begin(); it != table_.end(); it++) {
            it->second *= value;
        }
    }

    //! Multiply
    void mul(PreciseCounter const& val) {
        for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
            table_[it->first] *= it->second;
        }
    }

    //! Divide
    void div(PreciseCounter const& val) {
        for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
            table_[it->first] /= it->second;
        }
    }
};


//                              //
//      SMASlidingWindow        //
//                              //

static double checked_inv(u32 depth) {
    if (depth == 0) {
        NodeException err("Sliding window depth can't be zero.");
        BOOST_THROW_EXCEPTION(err);
    }
    return 1.0/depth;
}

//! Simple moving average implementation
template<class Frame>
struct SMASlidingWindow {
    typedef std::unique_ptr<Frame> PFrame;
    PFrame             sma_;
    const u32          depth_;
    const double       mul_;
    std::deque<PFrame> queue_;

    SMASlidingWindow(u32 depth)
        : depth_(depth)
        , mul_(checked_inv(depth))
    {
    }

    void add(PFrame sketch) {
        if (!sma_) {
            sma_.reset(new Frame(*sketch));
            queue_.push_back(std::move(sketch));
        } else {
            sma_->add(*sketch);
            queue_.push_back(std::move(sketch));
            if (queue_.size() > depth_) {
                auto removed = std::move(queue_.front());
                queue_.pop_front();
                sma_->sub(*removed);
            }
        }
    }

    PFrame forecast() const {
        PFrame res;
        if (queue_.size() < depth_) {
            // return empty response
            return std::move(res);
        }
        res.reset(new Frame(*sma_));
        res->mul(mul_);
        return std::move(res);
    }
};


//                              //
//      EWMASlidingWindow       //
//                              //


//! Exponentialy weighted moving average implementation
template<class Frame>
struct EWMASlidingWindow {
    typedef std::unique_ptr<Frame> PFrame;
    PFrame               ewma_;
    const double         decay_;
    int                  counter_;

    EWMASlidingWindow(double alpha)
        : decay_(alpha)
        , counter_(0)
    {
    }

    void add(PFrame sketch) {
        if (!ewma_) {
            ewma_.reset(new Frame(*sketch));
            counter_ = 1;
        } else if (counter_ < 10) {
            ewma_->add(*sketch);
            counter_++;
            if (counter_ == 10) {
                ewma_->mul(0.1);
            }
        } else {
            sketch->mul(decay_);
            ewma_->mul(1.0 - decay_);
            ewma_->add(*sketch);
        }
    }

    PFrame forecast() const {
        PFrame res;
        if (counter_ < 10) {
            // return empty response
            return std::move(res);
        }
        res.reset(new Frame(*ewma_));
        return std::move(res);
    }
};


//                                          //
//      DoubleExpSmoothingSlidingWindow     //
//                                          //


//! Holt-Winters moving average implementation
template<class Frame>
struct DoubleExpSmoothingSlidingWindow {
    typedef std::unique_ptr<Frame> PFrame;
    PFrame               baseline_;
    PFrame               slope_;
    const double         alpha_;
    const double         beta_;
    int                  counter_;

    /** C-tor
      * @param alpha smoothing coefficient
      */
    DoubleExpSmoothingSlidingWindow(double alpha, double beta)
        : alpha_(alpha)
        , beta_(beta)
        , counter_(0)
    {
    }

    void add(PFrame value) {
        switch(counter_) {
        case 0:
            std::swap(baseline_, value);
            counter_ = 1;
            break;
        case 1:
            slope_.reset(new Frame(*value));
            slope_->sub(*baseline_);
            baseline_ = std::move(value);
            counter_ = 2;
            break;
        default: {
                PFrame old_baseline(new Frame(*baseline_));
                PFrame old_slope = std::move(slope_);
                // Calculate new baseline
                {
                    PFrame new_baseline = std::move(value);
                    new_baseline->mul(alpha_);
                    old_baseline->add(*old_slope);
                    old_baseline->mul(1.0 - alpha_);
                    new_baseline->add(*old_baseline);
                    std::swap(new_baseline, baseline_);
                    std::swap(new_baseline, old_baseline);
                }
                // Calculate new slope
                slope_.reset(new Frame(*baseline_));
                slope_->sub(*old_baseline);
                slope_->mul(beta_);
                old_slope->mul(1.0 - beta_);
                slope_->add(*old_slope);
                break;
            }
        };
    }

    PFrame forecast() const {
        PFrame res;
        if (counter_ < 2) {
            // return empty response
            return std::move(res);
        }
        res.reset(new Frame(*baseline_));
        res->add(*slope_);
        return std::move(res);
    }
};


//                                      //
//      HoltWintersSlidingWindow        //
//                                      //

/** Holt-Winters implementation.
  * http://static.usenix.org/events/lisa00/full_papers/brutlag/brutlag_html/
  */
template<class Frame>
struct HoltWintersSlidingWindow {
    typedef std::unique_ptr<Frame> PFrame;
    PFrame               baseline_;
    PFrame               slope_;
    std::deque<PFrame>   seasonal_;
    const double         alpha_;
    const double         beta_;
    const double         gamma_;
    int                  counter_;
    int                  period_;

    HoltWintersSlidingWindow(double alpha, double beta, double gamma, int period)
        : alpha_(alpha)
        , beta_(beta)
        , gamma_(gamma)
        , counter_(0)
        , period_(period)
    {
    }

    void add(PFrame value) {
        if (counter_ == 0) {
            baseline_.reset(new Frame(*value));
            seasonal_.push_back(std::move(value));
        } else if (counter_ == 1) {
            slope_.reset(new Frame(*value));
            slope_->sub(*baseline_);
            baseline_.reset(new Frame(*value));
            seasonal_.push_back(std::move(value));
        } else if (counter_ < period_) {
            seasonal_.push_back(std::move(value));
        } else {
            PFrame seasonal = std::move(seasonal_.front());
            PFrame old_baseline;
            seasonal_.pop_front();
            // Calculate baseline
            {
                PFrame new_baseline(new Frame(*value));
                PFrame old_slope(new Frame(*slope_));
                new_baseline->sub(*seasonal);
                new_baseline->mul(alpha_);
                old_slope->add(*baseline_);
                old_slope->mul(1.0 - alpha_);
                new_baseline->add(*old_slope);
                old_baseline = std::move(baseline_);
                std::swap(new_baseline, baseline_);
            }
            // Calculate slope
            {
                PFrame new_slope(new Frame(*baseline_));
                PFrame old_slope = std::move(slope_);
                new_slope->sub(*old_baseline);
                new_slope->mul(beta_);
                old_slope->mul(1.0 - beta_);
                new_slope->add(*old_slope);
                std::swap(new_slope, slope_);
            }
            // Calculate seasonality
            {
                value->sub(*baseline_);
                value->mul(gamma_);
                seasonal->mul(1.0 - gamma_);
                value->add(*seasonal);
                seasonal_.push_back(std::move(value));
            }
        }
        counter_++;
    }

    PFrame forecast() const {
        PFrame res;
        if (counter_ < period_) {
            // return empty response
            return std::move(res);
        }
        res.reset(new Frame(*baseline_));
        res->add(*slope_);
        res->add(*seasonal_.back());
        return std::move(res);
    }
};


//                                  //
//      AnomalyDetectorPipeline     //
//                                  //

template<
    class Frame,                        // Frame type
    template<class F> class FMethod     // Forecasting method type
>
struct AnomalyDetectorPipeline : AnomalyDetectorIface {
    typedef FMethod<Frame>                  FcastMethod;
    typedef std::unique_ptr<Frame>          PFrame ;
    typedef std::unique_ptr<FcastMethod>    PSlidingWindow;

    HashFnFamily                hashes_;
    const u32              N;
    const u32              K;
    PFrame                      current_;
    PFrame                      error_;
    double                      F2_;
    double                      threshold_;
    PSlidingWindow              sliding_window_;

    AnomalyDetectorPipeline(u32 N, u32 K, double threshold, PSlidingWindow swindow)
        : hashes_(N, K)
        , N(N)
        , K(K)
        , F2_(0.0)
        , threshold_(threshold)
        , sliding_window_(std::move(swindow))
    {
        current_.reset(new Frame(hashes_));
    }

    void add(u64 id, double value) {
        current_->add(id, value);
    }

    //! Returns true if series is anomalous (approx)
    bool is_anomaly_candidate(u64 id) const {
        if (error_) {
            double estimate = error_->estimate(id);
            return estimate > F2_;
        }
        return false;
    }

    void move_sliding_window() {
        PFrame forecast = std::move(sliding_window_->forecast());
        if (forecast) {
            error_ = std::move(calculate_error(forecast, current_));
            F2_ = sqrt(error_->estimateF2())*threshold_;
        }
        sliding_window_->add(std::move(current_));
        current_.reset(new Frame(hashes_));
    }

    PFrame calculate_error(const PFrame &forecast, const PFrame &actual) {
        PFrame res;
        res.reset(new Frame(hashes_));
        res->diff(*forecast, *actual);
        return std::move(res);
    }
};

template<class Window, class Detector>
std::unique_ptr<AnomalyDetectorIface> create_detector(u32 N,
                                                      u32 K,
                                                      double threshold,
                                                      u32 window_size)
{
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(window_size));
    result.reset(new Detector(N, K, threshold, std::move(window)));
    return std::move(result);
}

//! Create approximate anomaly detector based on simple moving-average smothing
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_approx_sma(u32 N,
                                           u32 K,
                                           double threshold,
                                           u32 window_size)
{
    typedef AnomalyDetectorPipeline<CountingSketch, SMASlidingWindow>   Detector;
    typedef SMASlidingWindow<CountingSketch>                            Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(window_size));
    result.reset(new Detector(N, K, threshold, std::move(window)));
    return std::move(result);
}

//! Create precise anomaly detector based on simple moving-average smothing
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_precise_sma(double threshold,
                                            u32 window_size)
{
    typedef AnomalyDetectorPipeline<PreciseCounter, SMASlidingWindow>   Detector;
    typedef SMASlidingWindow<PreciseCounter>                            Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(window_size));
    result.reset(new Detector(1, 8, threshold, std::move(window)));
    return std::move(result);
}

//! Create approximate anomaly detector based on simple moving-average smothing or EWMA
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_approx_ewma(u32 N,
                                            u32 K,
                                            double threshold,
                                            double alpha)
{
    typedef AnomalyDetectorPipeline<CountingSketch, EWMASlidingWindow>  Detector;
    typedef EWMASlidingWindow<CountingSketch>                           Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha));
    result.reset(new Detector(N, K, threshold, std::move(window)));
    return std::move(result);
}

//! Create precise anomaly detector based on simple moving-average smothing or EWMA
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_precise_ewma(double threshold,
                                             double alpha)
{
    typedef AnomalyDetectorPipeline<PreciseCounter, EWMASlidingWindow>  Detector;
    typedef EWMASlidingWindow<PreciseCounter>                           Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha));
    result.reset(new Detector(1, 8, threshold, std::move(window)));
    return std::move(result);
}

//! Create precise anomaly detector based on simple moving-average smothing or EWMA
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_precise_double_exp_smoothing(
                                             double threshold,
                                             double alpha,
                                             double beta)
{
    typedef AnomalyDetectorPipeline<PreciseCounter, DoubleExpSmoothingSlidingWindow>  Detector;
    typedef DoubleExpSmoothingSlidingWindow<PreciseCounter>                           Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha, beta));
    result.reset(new Detector(1, 8, threshold, std::move(window)));
    return std::move(result);
}

std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_approx_double_exp_smoothing(
                                         u32 N,
                                         u32 K,
                                         double threshold,
                                         double alpha,
                                         double beta)
{
    typedef AnomalyDetectorPipeline<CountingSketch, DoubleExpSmoothingSlidingWindow>  Detector;
    typedef DoubleExpSmoothingSlidingWindow<CountingSketch>                           Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha, beta));
    result.reset(new Detector(N, K, threshold, std::move(window)));
    return std::move(result);
}

//! Create precise anomaly detector based on simple moving-average smothing or EWMA
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_precise_holt_winters(
                                             double threshold,
                                             double alpha,
                                             double beta,
                                             double gamma,
                                             int period)
{
    typedef AnomalyDetectorPipeline<PreciseCounter, HoltWintersSlidingWindow>         Detector;
    typedef HoltWintersSlidingWindow<PreciseCounter>                                  Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha, beta, gamma, period));
    result.reset(new Detector(1, 8, threshold, std::move(window)));
    return std::move(result);
}

//! Create precise anomaly detector based on simple moving-average smothing or EWMA
std::unique_ptr<AnomalyDetectorIface>
    AnomalyDetectorUtil::create_approx_holt_winters(
                                             u32 N,
                                             u32 K,
                                             double threshold,
                                             double alpha,
                                             double beta,
                                             double gamma,
                                             int period)
{
    typedef AnomalyDetectorPipeline<PreciseCounter, HoltWintersSlidingWindow>         Detector;
    typedef HoltWintersSlidingWindow<PreciseCounter>                                  Window;
    std::unique_ptr<AnomalyDetectorIface> result;
    std::unique_ptr<Window> window(new Window(alpha, beta, gamma, period));
    result.reset(new Detector(N, K, threshold, std::move(window)));
    return std::move(result);
}

}
}

