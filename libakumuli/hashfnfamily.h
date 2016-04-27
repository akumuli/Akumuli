#pragma once

#include "akumuli_def.h"

#include <cstdint>
#include <random>
#include <vector>

namespace Akumuli {

//! Family of 4-universal hash functions
struct HashFnFamily {
    const u32 N;
    const u32 K;
    //! Tabulation based hash fn used, N tables should be generated using RNG in c-tor
    std::vector<std::vector<unsigned short>> table_;

    //! C-tor. N - number of different hash functions, K - number of values (should be a power of two)
    HashFnFamily(u32 N, u32 K);

    //! Calculate hash value in range [0, K)
    u32 hash(int ix, u64 key) const;

private:
    u32 hash32(int ix, u32 key) const;
};

}  // namespace
