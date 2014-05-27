#include <iostream>
#include <vector>
#include <algorithm>

#include "rdtsc.h"

#include "cache.h"

/** Benchmark runner class.
  * Base class for all tests.
  * @param Derived derived class
  * @param NIter number of iterations
  */
class BenchmarkRunner {
    std::vector<unsigned long long> results_;
    unsigned long long median_, min_;
    int niter_;
    CPUCounter cnt_;
    int iter_;
public:

    BenchmarkRunner(int niter)
        : results_(niter, 0)
        , niter_(niter)
        , iter_(0)
    {}

    // FIXME: it's not portable
    void resume() noexcept __attribute__((always_inline)) {
        cnt_.resume();
    }

    void pause() noexcept __attribute__((always_inline)) {
        cnt_.pause();
    }

    //! Next iteration - save counter
    void advance() noexcept {
        auto res = cnt_.elapsed();
        results_[iter_++] = res;
        cnt_.reset();
    }

    bool done() const noexcept {
        return niter_ == iter_;
    }

    template <class Fn>
    void run(const char* name, Fn fn) {

        fn(*this);

        // process results
        std::sort(results_.begin(), results_.end());
        median_ = results_[niter_ / 2];
        min_ = results_[0];

        std::cout << name << " min=" << min_ << " median=" << median_ << std::endl;
    }
};

using namespace Akumuli;


int main() {
    BenchmarkRunner bench(1000000);
    bench.run("Test benchmark", [] (BenchmarkRunner& b) {
        unsigned long long x = 0;
        while(!b.done()) {
            b.resume();
            x++;
            b.pause();
            b.advance();
        }
    });
    return 0;
}
