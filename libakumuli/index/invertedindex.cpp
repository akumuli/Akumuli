/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include "invertedindex.h"

#include <random>
#include <memory>
#include <algorithm>

namespace Akumuli {

static const int CARDINALITY = 3;

TwoUnivHashFnFamily::TwoUnivHashFnFamily(int cardinality, size_t modulo)
    : INTERNAL_CARDINALITY_(cardinality)
    , prime(2147483647)  // 2^31-1
    , modulo(modulo)
{
    a.resize(INTERNAL_CARDINALITY_);
    b.resize(INTERNAL_CARDINALITY_);
    std::random_device randdev;
    std::minstd_rand generator(randdev());
    std::uniform_int_distribution<> distribution;
    for (int i = 0; i < INTERNAL_CARDINALITY_; i++) {
        a[i] = distribution(generator);
        b[i] = distribution(generator);
    }
}

u64 TwoUnivHashFnFamily::hash(int ix, u64 value) const {
    return ((a[ix]*value + b[ix]) % prime) % modulo;
}

void Postings::append(aku_ParamId id) {
    auto it = counters_.find(id);
    if (it != counters_.end()) {
        it->second++;
    } else {
        counters_[id] = 1u;
    }
}

size_t Postings::get_size() const {
    return counters_.size();
}

size_t Postings::get_count(aku_ParamId id) const {
    auto it = counters_.find(id);
    if (it != counters_.end()) {
        return it->second;
    }
    return 0u;
}

void Postings::merge(const Postings& other) {
    std::unordered_map<aku_ParamId, size_t> tmp;
    for (auto kv: counters_) {
        auto it = other.counters_.find(kv.first);
        if (it != other.counters_.end()) {
            tmp[kv.first] = std::min(kv.second, it->second);
        }
    }
    std::swap(tmp, counters_);
}

InvertedIndex::InvertedIndex(const size_t table_size)
    : table_hash_(CARDINALITY, table_size)
    , table_size_(table_size)
{
    table_.resize(table_size);
    for (auto i = 0u; i < table_size; i++) {
        std::unique_ptr<Postings> ptr;
        ptr.reset(new Postings());
        table_.at(i) = std::move(ptr);
    }
}

static u64 sdbm(const char* begin, const char* end) {
    unsigned long hash = 0;
    for (auto it = begin; it != end; it++) {
        hash = *it + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

void InvertedIndex::append(aku_ParamId id, const char* begin, const char* end) {
    auto hash = sdbm(begin, end);
    for (int i = 0; i < CARDINALITY; i++) {
        auto ith_hash = table_hash_.hash(i, hash);
        table_.at(ith_hash)->append(id);
    }
}

std::vector<std::pair<aku_ParamId, size_t>> InvertedIndex::get_count(const char *begin, const char *end) {
    auto hash = sdbm(begin, end);
    std::vector<std::unique_ptr<Postings>> postings;
    for (int i = 0; i < CARDINALITY; i++) {
        auto ith_hash = table_hash_.hash(i, hash);
        Postings& original = *table_.at(ith_hash);
        std::unique_ptr<Postings> copy;
        copy.reset(new Postings());
        *copy = original;
        postings.push_back(std::move(copy));
    }

    std::sort(postings.begin(), postings.end(),
    [](const std::unique_ptr<Postings>& lhs, const std::unique_ptr<Postings>& rhs) {
        return lhs->get_size() < rhs->get_size();
    });

    auto merged = std::move(postings[0]);
    auto pbegin = postings.begin();
    pbegin++;
    while(pbegin != postings.end()) {
        merged->merge(**pbegin);
        pbegin++;
    }

    std::vector<std::pair<aku_ParamId, size_t>> results;

    for (auto kv: merged->counters_) {
        results.push_back(kv);
    }

    return results;
}

}  // namespace
