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
            row[col] = abs(lrow[col] - rrow[col]);
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

// ExactCounter //

ExactCounter::ExactCounter(HashFnFamily const& hf) {
}

ExactCounter::ExactCounter(ExactCounter const& cs)
    : table_(cs.table_)
{
}

void ExactCounter::add(uint64_t id, double value) {
    table_[id] += value;
}

//! Unbiased value estimator
double ExactCounter::estimate(uint64_t id) const {
    auto it = table_.find(id);
    if (it != table_.end()) {
        return it->second;
    }
    return 0.;
}

//! Second moment estimator
double ExactCounter::estimateF2() const {
    double sum = std::accumulate(table_.begin(), table_.end(), 0.0,
                                 [](double acc, std::pair<uint64_t, double> pval) {
        return acc + pval.second*pval.second;
    });
    return sqrt(sum);
}

//! current sketch <- absolute difference between two arguments
void ExactCounter::diff(ExactCounter const& lhs, ExactCounter const& rhs) {
    for(auto it = lhs.table_.begin(); it != lhs.table_.end(); it++) {
        auto itrhs = rhs.table_.find(it->first);
        double rhsval = 0.;
        if (itrhs == rhs.table_.end()) {
            rhsval = itrhs->second;
        }
        table_[it->first] = it->second + rhsval;
    }
}

//! Add sketch
void ExactCounter::add(ExactCounter const& val) {
    for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
        table_[it->first] += it->second;
    }
}

//! Substract sketch
void ExactCounter::sub(ExactCounter const& val) {
    for(auto it = val.table_.begin(); it != val.table_.end(); it++) {
        table_[it->first] -= it->second;
    }
}

//! Multiply sketch by value
void ExactCounter::mul(double value) {
    for(auto it = table_.begin(); it != table_.end(); it++) {
        it->second *= value;
    }
}

// CountingSketchProcessor //


CountingSketchProcessor::CountingSketchProcessor(uint32_t N, uint32_t K, double threshold, std::unique_ptr<ForecastingMethod> swindow)
    : hashes_(N, K)
    , N(N)
    , K(K)
    , F2_(0.0)
    , threshold_(threshold)
    , sliding_window_(std::move(swindow))
{
    current_.reset(new CountingSketch(hashes_));
}

void CountingSketchProcessor::add(uint64_t id, double value) {
    current_->add(id, value);
}

//! Returns true if series is anomalous (approx)
bool CountingSketchProcessor::is_anomaly_candidate(uint64_t id) const {
    if (error_) {
        double estimate = error_->estimate(id);
        return estimate > F2_;
    }
    return false;
}

void CountingSketchProcessor::move_sliding_window() {
    PSketch forecast = std::move(sliding_window_->forecast());
    if (forecast) {
        error_ = std::move(calculate_error(forecast, current_));
        F2_ = sqrt(error_->estimateF2())*threshold_;
    }
    sliding_window_->add(std::move(current_));
    current_.reset(new CountingSketch(hashes_));
}

CountingSketchProcessor::PSketch CountingSketchProcessor::calculate_error(const CountingSketchProcessor::PSketch& forecast,
                                                                          const CountingSketchProcessor::PSketch& actual)
{
    PSketch res;
    res.reset(new CountingSketch(hashes_));
    res->diff(*forecast, *actual);
    return std::move(res);
}

}
}

