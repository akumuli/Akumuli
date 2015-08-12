#pragma once
#include <cinttypes>
#include <vector>

namespace Akumuli {
namespace QP {

//! Family of 4-universal hash functions
struct HashFnFamily {
    std::vector<std::vector<unsigned short>> table_;

    //! C-tor. N - number of different hash functions, K - number of values (should be a power of two)
    HashFnFamily(int N, int K);

    //! Calculate hash value in range [0, K)
    uint32_t hash(int ix, uint64_t key);

private:
    uint32_t hash32(int ix, uint32_t key);
};

struct AnomalyDetector {

};

}
}
