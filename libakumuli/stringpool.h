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
 */

#pragma once

#include <vector>
#include <deque>
#include <unordered_map>
#include <mutex>
#include <atomic>

#include "akumuli_def.h"

namespace Akumuli {


//! Offset inside string-pool
struct StringPoolOffset {
    //! Offset of the buffer
    size_t buffer_offset;
    //! Offset inside buffer
    size_t offset;
};

struct StringPool {

    typedef std::pair<const char*, int> StringT;
    const int MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME*0x1000;

    std::deque<std::vector<char>> pool;
    mutable std::mutex pool_mutex;
    std::atomic<size_t> counter;

    StringT add(const char* begin, const char *end, uint64_t payload);

    //! Get number of stored strings atomically
    size_t size() const;

    /** Find all series that match regex.
      * @param regex is a regullar expression
      * @param outoffset can be used to retreive offset of the processed data or start search from
      *        particullar point in the string-pool
      */
    std::vector<StringT> regex_match(const char* regex, StringPoolOffset* outoffset = nullptr) const;
};

struct StringTools {
    //! Pooled string
    typedef std::pair<const char*, int> StringT;

    static size_t hash(StringT str);
    static bool equal(StringT lhs, StringT rhs);

    typedef std::unordered_map<StringT, uint64_t,
                               decltype(&StringTools::hash),
                               decltype(&StringTools::equal)> TableT;

    //! Inverted table type (id to string mapping)
    typedef std::unordered_map<uint64_t, StringT> InvT;

    static TableT create_table(size_t size);

    static uint64_t extract_id_from_pool(StringPool::StringT res);
};

}
