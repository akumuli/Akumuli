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

    void add(uint64_t id, double value);

    //! Unbiased value estimator
    double estimate(uint64_t id) const;

    //! Second moment estimator
    double estimateF2() const;

    double& at(int row, int col);
};


// TODO: algorithm should be parametrized (SMA used now for simplicity)
struct CountingSketchProcessor {
    typedef std::unique_ptr<CountingSketch> PSketchWindow;

    HashFnFamily hashes_;
    const uint32_t N;
    const uint32_t K;
    const uint32_t DEPTH;
    PSketchWindow window_;
    std::deque<PSketchWindow> old_;
    PSketchWindow error_;
    double F2_;
    double threshold_;

    CountingSketchProcessor(uint32_t N, uint32_t K, double threshold)
        : hashes_(N, K)
        , N(N)
        , K(K)
        , DEPTH(5)
        , F2_(0.0)
        , threshold_(threshold)
    {
        window_.reset(new CountingSketch(hashes_));
    }

    void add(uint64_t id, double value) {
        window_->add(id, value);
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
        PSketchWindow forecast = std::move(SMA());
        if (forecast) {
            PSketchWindow error = std::move(calculate_error(forecast, window_));
            error_ = std::move(error);
            F2_ = sqrt(error_->estimateF2())*threshold_;
        }

        old_.push_back(std::move(window_));
        window_.reset(new CountingSketch(hashes_));

        if (old_.size() > DEPTH) {
            old_.pop_front();
        }
    }

    PSketchWindow SMA() const {
        PSketchWindow res;
        if (old_.size() < DEPTH) {
            // return empty response
            return std::move(res);
        }
        res.reset(new CountingSketch(hashes_));
        for (auto it = old_.size() - DEPTH; it < old_.size(); it++) {
            for (auto row = 0u; row < N; row++) {
                for (auto col = 0u; col < K; col++) {
                    res->at(row, col) += old_[it]->at(row, col);
                }
            }
        }
        for (auto row = 0u; row < N; row++) {
            for (auto col = 0u; col < K; col++) {
                res->at(row, col) /= double(DEPTH);
            }
        }
        return std::move(res);
    }

    PSketchWindow calculate_error(const PSketchWindow& forecast, const PSketchWindow& error) {
        PSketchWindow res;
        res.reset(new CountingSketch(hashes_));

        for (auto row = 0u; row < N; row++) {
            for (auto col = 0u; col < K; col++) {
                res->at(row, col) = forecast->at(row, col) - error->at(row, col);
            }
        }

        return std::move(res);
    }
};

}
}
