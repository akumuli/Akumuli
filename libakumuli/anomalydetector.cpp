#include "anomalydetector.h"
#include <random>

namespace Akumuli {
namespace QP {

HashFnFamily::HashFnFamily(int N, int K)
{
    std::random_device randdev;
    std::mt19937 generator(randdev);
    std::uniform_int_distribution distribution;
    for (int i = 0; i < N; i++) {
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

uint32_t HashFnFamily::hash32(int ix, uint32_t key) {
    auto hi16 = key >> 16;
    auto lo16 = key & 0xFFFF;
    auto hilo = combine(hi16, lo16);
    return table_[ix][lo16] ^ table_[ix][hi16] ^ table_[ix][hilo];
}

uint32_t HashFnFamily::hash(int ix, uint64_t key) {
    auto hi32 = key >> 32;
    auto lo32 = key & 0xFFFFFFFF;
    auto hilo = combine(hi32, lo32);

    auto hi32hash = hash(ix, hi32);
    auto lo32hash = hash(ix, lo32);
    auto hilohash = hash(ix, hilo);

    return hi32hash ^ lo32hash ^ hilohash;
}

}

