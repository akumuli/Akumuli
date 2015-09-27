#pragma once

#include <random>
#include <vector>
#include <cstdint>

namespace Akumuli {

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




}
