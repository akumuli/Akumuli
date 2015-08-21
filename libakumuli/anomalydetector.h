#pragma once
#include <cinttypes>
#include <vector>
#include <deque>
#include <unordered_map>
#include <memory>
#include <math.h>

namespace Akumuli {
namespace QP {

//! Family of 4-universal hash functions
struct HashFnFamily {
    const uint32_t N;
    const uint32_t K;
    //! Tabulation based hash fn used, N tables should be generated using RNG in c-tor
    std::vector<std::vector<unsigned short>> table_;

    //! C-tor. N - number of different hash functions, K - number of values (should be a power of two)
    HashFnFamily(uint32_t N, uint32_t K);

    //! Calculate hash value in range [0, K)
    uint32_t hash(int ix, uint64_t key) const;

private:
    uint32_t hash32(int ix, uint32_t key) const;
};

struct CountingSketch {
    HashFnFamily const& hashes_;
    const uint32_t N;
    const uint32_t K;
    double sum_;
    std::vector<std::vector<double>> tables_;

    CountingSketch(HashFnFamily const& hf);

    CountingSketch(CountingSketch const& cs);

    void _update_sum();

    void add(uint64_t id, double value);

    //! Unbiased value estimator
    double estimate(uint64_t id) const;

    //! Second moment estimator
    double estimateF2() const;

    //! current sketch <- absolute difference between two arguments
    void diff(CountingSketch const& lhs, CountingSketch const& rhs);

    //! Add sketch
    void add(CountingSketch const& val);

    //! Substract sketch
    void sub(CountingSketch const& val);

    //! Multiply sketch by value
    void mul(double value);
};

struct ExactCounter {
    std::unordered_map<uint64_t, double> table_;

    //! C-tor. Parameter `hf` is unused for the sake of interface unification.
    ExactCounter(HashFnFamily const& hf);

    ExactCounter(ExactCounter const& cs);

    void _update_sum();

    void add(uint64_t id, double value);

    //! Unbiased value estimator
    double estimate(uint64_t id) const;

    //! Second moment estimator
    double estimateF2() const;

    //! current sketch <- absolute difference between two arguments
    void diff(ExactCounter const& lhs, ExactCounter const& rhs);

    //! Add sketch
    void add(ExactCounter const& val);

    //! Substract sketch
    void sub(ExactCounter const& val);

    //! Multiply sketch by value
    void mul(double value);
};


struct ForecastingMethod {
    typedef std::unique_ptr<CountingSketch> PSketch;

    virtual void add(PSketch sketch) = 0;

    virtual PSketch forecast() const = 0;
};

//! Simple moving average implementation
struct SMASlidingWindow : ForecastingMethod {

    PSketch             sma_;
    const uint32_t      depth_;
    const double        mul_;
    std::deque<PSketch> queue_;

    SMASlidingWindow(uint32_t depth)
        : depth_(depth)
        , mul_(1.0/depth)
    {
    }

    void add(PSketch sketch) {
        if (!sma_) {
            sma_.reset(new CountingSketch(*sketch));
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

    PSketch forecast() const {
        PSketch res;
        if (queue_.size() < depth_) {
            // return empty response
            return std::move(res);
        }
        res.reset(new CountingSketch(*sma_));
        res->mul(mul_);
        return std::move(res);
    }
};

//! Exponentialy weighted moving average implementation
struct EWMASlidingWindow : ForecastingMethod {

    PSketch              ewma_;
    const double         decay_;
    int                  counter_;

    EWMASlidingWindow(uint32_t depth)
        : decay_(1.0/(double(depth) + 1.0))
        , counter_(0.0)
    {
    }

    void add(PSketch sketch) {
        if (!ewma_) {
            ewma_.reset(new CountingSketch(*sketch));
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

    PSketch forecast() const {
        PSketch res;
        if (counter_ < 10) {
            // return empty response
            return std::move(res);
        }
        res.reset(new CountingSketch(*ewma_));
        return std::move(res);
    }
};

struct CountingSketchProcessor {
    typedef std::unique_ptr<CountingSketch> PSketch;
    typedef std::unique_ptr<ForecastingMethod>  PSlidingWindow;

    HashFnFamily                hashes_;
    const uint32_t              N;
    const uint32_t              K;
    PSketch                     current_;
    PSketch                     error_;
    double                      F2_;
    double                      threshold_;
    PSlidingWindow              sliding_window_;

    CountingSketchProcessor(uint32_t N, uint32_t K, double threshold, std::unique_ptr<ForecastingMethod> swindow);

    void add(uint64_t id, double value);

    //! Returns true if series is anomalous (approx)
    bool is_anomaly_candidate(uint64_t id) const;

    void move_sliding_window();

    PSketch calculate_error(const PSketch& forecast, const PSketch& actual);
};

}
}
