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

#include "akumuli_def.h"

namespace Akumuli {

struct StringPool {

    typedef std::pair<const char*, int> StringT;
    const int MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME*0x1000;

    std::deque<std::vector<char>> pool;
    mutable std::mutex pool_mutex;

    StringT add(const char* begin, const char *end, uint64_t payload);
    std::vector<StringT> regex_match(const char* regex) const;
};

struct StringTools {
    //! Pooled string
    typedef std::pair<const char*, int> StringT;

    static size_t hash(StringT str);
    static bool equal(StringT lhs, StringT rhs);

    typedef std::unordered_map<StringT, uint64_t,
                               decltype(&StringTools::hash),
                               decltype(&StringTools::equal)> TableT;

    static TableT create_table(size_t size);
};

}
