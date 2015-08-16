#include "anomalydetector.h"
#include <random>
#include <stdexcept>

#include <boost/exception/all.hpp>

namespace Akumuli {
namespace QP {

HashFnFamily::HashFnFamily(uint32_t N, uint32_t K)
    : N(N)
    , K(K)
{
    // N should be odd
    if (N % 2 == 0) {
        std::runtime_error err("invalid argument N (should be odd)");
        BOOST_THROW_EXCEPTION(err);
    }
    // K should be a power of two
    auto mask = K-1;
    if ((mask&K) != 0) {
        std::runtime_error err("invalid argument K (should be a power of two)");
        BOOST_THROW_EXCEPTION(err);
    }
    // Generate tables
    std::random_device randdev;
    std::mt19937 generator(randdev());
    std::uniform_int_distribution<> distribution;
    for (uint32_t i = 0; i < N; i++) {
        std::vector<unsigned short> col;
        auto mask = K-1;
        for (int j = 0; j < 0x10000; j++) {
            int value = distribution(generator);
            col.push_back((uint32_t)mask&value);
        }
        table_.push_back(col);
    }
}

static uint32_t combine(uint32_t hi, uint32_t lo) {
    return (uint32_t)(2 - (int)hi + (int)lo);
}

uint32_t HashFnFamily::hash32(int ix, uint32_t key) const {
    auto hi16 = key >> 16;
    auto lo16 = key & 0xFFFF;
    auto hilo = combine(hi16, lo16);
    return table_[ix][lo16] ^ table_[ix][hi16] ^ table_[ix][hilo];
}

uint32_t HashFnFamily::hash(int ix, uint64_t key) const {
    auto hi32 = key >> 32;
    auto lo32 = key & 0xFFFFFFFF;
    auto hilo = combine(hi32, lo32);

    auto hi32hash = hash32(ix, hi32);
    auto lo32hash = hash32(ix, lo32);
    auto hilohash = hash32(ix, hilo);

    return hi32hash ^ lo32hash ^ hilohash;
}

// SketchWindow
CountingSketch::CountingSketch(HashFnFamily const& hf)
    : hashes_(hf)
    , N(hf.N)
    , K(hf.K)
    , sum_(0.0)
{
    for (uint32_t i = 0u; i < N; i++) {
        std::vector<double> row;
        row.resize(K, 0.0);
        tables_.push_back(std::move(row));
    }
}

CountingSketch::CountingSketch(CountingSketch const& cs)
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

void CountingSketch::add(uint64_t id, double value) {
    sum_ += value;
    for (uint32_t i = 0; i < N; i++) {
        // calculate hash from id to K
        uint32_t hash = hashes_.hash(i, id);
        tables_[i][hash] += value;
    }
}

void CountingSketch::_update_sum() {
    sum_ = 0.0;
    for (auto val: tables_[0]) {
        sum_ += val;
    }
}

void CountingSketch::diff(CountingSketch const& lhs, CountingSketch const& rhs) {
    for (auto ixrow = 0u; ixrow < N; ixrow++) {
        std::vector<double>& row = tables_[ixrow];
        std::vector<double> const& lrow = lhs.tables_[ixrow];
        std::vector<double> const& rrow = rhs.tables_[ixrow];
        for (auto col = 0u; col < K; col++) {
            row[col] = lrow[col] - rrow[col];
        }
    }
    _update_sum();
}

void CountingSketch::add(CountingSketch const& val) {
    for (auto ixrow = 0u; ixrow < N; ixrow++) {
        std::vector<double>& row = tables_[ixrow];
        std::vector<double> const& rval = val.tables_[ixrow];
        for (auto col = 0u; col < K; col++) {
            row[col] = row[col] + rval[col];
        }
    }
    _update_sum();
}

void CountingSketch::sub(CountingSketch const& val) {
    for (auto ixrow = 0u; ixrow < N; ixrow++) {
        std::vector<double>& row = tables_[ixrow];
        std::vector<double> const& rval = val.tables_[ixrow];
        for (auto col = 0u; col < K; col++) {
            row[col] = row[col] - rval[col];
        }
    }
    _update_sum();
}

void CountingSketch::mul(double value) {
    for (auto ixrow = 0u; ixrow < N; ixrow++) {
        std::vector<double>& row = tables_[ixrow];
        for (auto col = 0u; col < K; col++) {
            row[col] *= value;
        }
    }
    _update_sum();
}

double CountingSketch::estimate(uint64_t id) const {
    std::vector<double> results;
    for (uint32_t i = 0u; i < N; i++) {
        uint32_t hash = hashes_.hash(i, id);
        double value = tables_[i][hash];
        double estimate = (value - sum_/K)/(1. - 1./K);
        results.push_back(estimate);
    }
    std::sort(results.begin(), results.end());
    return results[N/2];
}

double CountingSketch::estimateF2() const {
    std::vector<double> results;
    auto f = 1./(K - 1);
    for (uint32_t i = 0u; i < N; i++) {
        double rowsum = std::accumulate(tables_[i].begin(), tables_[i].end(), 0.0, [](double acc, double val) {
            return acc + val*val;
        });
        double res = K*f*sqrt(rowsum) - f*sum_*sum_;
        results.push_back(res);
    }
    std::sort(results.begin(), results.end());
    return results[N/2];
}

}
}

