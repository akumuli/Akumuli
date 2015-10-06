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
#pragma once
#include "akumuli.h"
#include "hashfnfamily.h"

#include <vector>
#include <unordered_map>
#include <memory>

namespace Akumuli {

struct TwoUnivHashFnFamily {
    const int INTERNAL_CARDINALITY_;
    std::vector<uint64_t> a;
    std::vector<uint64_t> b;
    uint64_t prime;
    uint64_t modulo;

    TwoUnivHashFnFamily(int cardinality, size_t modulo);

    uint64_t hash(int ix, uint64_t value) const;
};

/** Posting list.
 * In case of time-series data posting list is a pair of time-series Id and time-stamp
 * of the occurence.
 */
struct Postings {
    std::unordered_map<aku_ParamId, size_t> counters_;

    void append(aku_ParamId id);

    size_t get_count(aku_ParamId id) const;

    size_t get_size() const;

    void merge(const Postings &other);
};


/** Inverted index.
 * One dimension of the inv-index is fixed (table size). Only postings can grow.
 */
struct InvertedIndex {
    TwoUnivHashFnFamily table_hash_;

    //! Size of the table
    const size_t table_size_;

    //! Hash to postings list mapping.
    std::vector<std::unique_ptr<Postings>> table_;

    //! C-tor. Argument `table_size` should be a power of two.
    InvertedIndex(const size_t table_size);

    //! Add value to index
    void append(aku_ParamId id, const char* begin, const char* end);

    std::vector<std::pair<aku_ParamId, size_t> > get_count(const char* begin, const char* end);
};

}  // namespace

