#pragma once
#include <cinttypes>
#include <vector>
#include <deque>
#include <memory>

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

struct SketchWindow {
    HashFnFamily const& hashes_;
    const uint32_t N;
    const uint32_t K;
    double sum_;
    std::vector<std::vector<double>> tables_;

    SketchWindow(HashFnFamily const& hf);

    void add(uint64_t id, double value);

    //! Unbiased value estimator
    double estimate(uint64_t id) const;

    //! Second moment estimator
    double estimateF2() const;

    double& at(int row, int col);
};


// TODO: algorithm should be parametrized (SMA used now for simplicity)
struct AnomalyDetector {
    HashFnFamily hashes_;
    const uint32_t N;
    const uint32_t K;
    const uint32_t DEPTH;
    typedef std::unique_ptr<SketchWindow> PSketchWindow;
    PSketchWindow window_;
    std::deque<PSketchWindow> old_;
    std::deque<PSketchWindow> forecast_;
    std::deque<PSketchWindow> error_;

    AnomalyDetector(uint32_t N, uint32_t K)
        : hashes_(N, K)
        , N(N)
        , K(K)
        , DEPTH(5)
    {
    }

    void add(uint64_t id, double value) {
        window_->add(id, value);
    }

    void move_sliding_window() noexcept {
        PSketchWindow forecast = std::move(SMA());
        if (forecast) {
            PSketchWindow diff = std::move(calculate_error(forecast, window_));
            // INVARIANT: foreacast_ and error_ always updated together
            forecast_.push_back(std::move(forecast));
            error_.push_back(std::move(diff));
        }
        old_.push_back(std::move(window_));
        window_.reset(new SketchWindow(hashes_));

        if (old_.size() > DEPTH) {
            old_.pop_front();
        }
        if (forecast_.size() > DEPTH) {
            forecast_.pop_front();
            error_.pop_front();
        }
    }

    PSketchWindow SMA() const {
        PSketchWindow res;
        if (old_.size() < DEPTH) {
            // return empty response
            return std::move(res);
        }
        res.reset(new SketchWindow(hashes_));
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
        res.reset(new SketchWindow(hashes_));

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
