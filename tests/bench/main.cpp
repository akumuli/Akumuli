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
template<class Derived, int NIter>
class BenchmarkRunner {
    std::vector<unsigned long long> results_;
    unsigned long long median_, min_;
protected:
    std::string name_;
public:

    BenchmarkRunner()
        : results_(NIter, 0)
    {}

    void execute() {
        CPUCounter cnt;
        for (int i = 0; i < NIter; i++) {
            cnt.reset();
            static_cast<Derived*>(this)->run();
            auto res = cnt.elapsed();
            results_[i] = res;
        }

        // process results
        std::sort(results_.begin(), results_.end());
        median_ = results_[NIter / 2];
        min_ = results_[0];

        std::cout << name_ << " min=" << min_ << " median=" << median_ << std::endl;
    }
};

using namespace Akumuli;

struct GenFindTest : BenchmarkRunner<GenFindTest, 100000> {
    Generation gen_;

    GenFindTest()
        : gen_(TimeDuration::make(1000L), 10000000u)
    {
        name_ = "Generation(find)";

        for (int i = 0; i < 100; i++) {
            gen_.add(TimeStamp::make(10L), 5, i);
        }
    }

    void run() {
        EntryOffset off[50];
        gen_.find(TimeStamp::make(10L), 5, off, 10, 50);
    }
};

int main() {
    GenFindTest gen_find_test;
    gen_find_test.execute();
    return 0;
}
