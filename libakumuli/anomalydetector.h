#pragma once
#include <cinttypes>
#include <vector>
#include <deque>
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

//! Simple moving average implementation
struct SMASlidingWindow {
    typedef std::unique_ptr<CountingSketch> PSketch;

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


// TODO: algorithm should be parametrized (SMA used now for simplicity)
template<class SlidingWindow>
struct CountingSketchProcessor {
    typedef std::unique_ptr<CountingSketch> PSketch;
    typedef std::unique_ptr<SlidingWindow>  PSlidingWindow;

    HashFnFamily                hashes_;
    const uint32_t              N;
    const uint32_t              K;
    PSketch                     current_;
    PSketch                     error_;
    double                      F2_;
    double                      threshold_;
    PSlidingWindow              sliding_window_;

    CountingSketchProcessor(uint32_t N, uint32_t K, double threshold, std::unique_ptr<SlidingWindow> swindow)
        : hashes_(N, K)
        , N(N)
        , K(K)
        , F2_(0.0)
        , threshold_(threshold)
        , sliding_window_(std::move(swindow))
    {
        current_.reset(new CountingSketch(hashes_));
    }

    void add(uint64_t id, double value) {
        current_->add(id, value);
    }

    //! Returns true if series is anomalous (approx)
    bool is_anomaly_candidate(uint64_t id) const {
        if (error_) {
            double estimate = error_->estimate(id);
            return estimate > F2_;
        }
        return false;
    }

    void move_sliding_window() {
        PSketch forecast = std::move(sliding_window_->forecast());
        if (forecast) {
            error_ = std::move(calculate_error(forecast, current_));
            F2_ = sqrt(error_->estimateF2())*threshold_;
        }
        sliding_window_->add(std::move(current_));
        current_.reset(new CountingSketch(hashes_));
    }

    PSketch calculate_error(const PSketch& forecast, const PSketch& actual) {
        PSketch res;
        res.reset(new CountingSketch(hashes_));
        res->diff(*forecast, *actual);
        return std::move(res);
    }
};

}
}
