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
#include <memory>

namespace Akumuli {

/** Posting list.
 * In case of time-series data posting list is a pair of time-series Id and time-stamp
 * of the occurence.
 */
struct Postings {
    // Maybe I should use D-gap compression scheme - http://bmagic.sourceforge.net/dGap.html
    // or something else. Maybe it will work fine without any compression at all.
    std::vector<aku_ParamId>   paramids_;
    std::vector<aku_Timestamp> timestamps_;
};


/** Inverted index.
 * One dimension of the inv-index is fixed (table size). Only postings can grow.
 */
struct InvertedIndex {
    //! Size of the table
    const size_t table_size_;
    //! Family of 4-universal hash functions
    HashFnFamily hashes_;

    //! Hash to postings list mapping.
    std::vector<std::unique_ptr<Postings>> table_;

    //! C-tor. Argument `table_size` should be a power of two.
    InvertedIndex(const size_t table_size);
};

}  // namespace

