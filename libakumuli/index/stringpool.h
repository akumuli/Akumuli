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

#include <atomic>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "akumuli_def.h"

namespace Akumuli {


//! Offset inside string-pool
struct StringPoolOffset {
    //! Offset of the buffer
    size_t buffer_offset;
    //! Offset inside buffer
    size_t offset;
};

//                        //
//   Legacy String Pool   //
//                        //

struct LegacyStringPool {

    typedef std::pair<const char*, int> StringT;
    const int MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME * 0x1000;

    std::deque<std::vector<char>> pool;
    mutable std::mutex            pool_mutex;
    std::atomic<size_t>           counter;

    LegacyStringPool();
    LegacyStringPool(LegacyStringPool const&) = delete;
    LegacyStringPool& operator=(LegacyStringPool const&) = delete;

    StringT add(const char* begin, const char* end);

    //! Get number of stored strings atomically
    size_t size() const;

    /** Find all series that match regex.
      * @param regex is a regullar expression
      * @param outoffset can be used to retreive offset of the processed data or start search from
      *        particullar point in the string-pool
      */
    std::vector<StringT> regex_match(const char* regex, StringPoolOffset* outoffset = nullptr,
                                     size_t* psize = nullptr) const;
};

//                       //
//      String Pool      //
//                       //

typedef std::pair<const char*, u32> StringT;

class StringPool {
public:
    const u64 MAX_BIN_SIZE = AKU_LIMITS_MAX_SNAME * 0x1000;  // 8Mb

    std::deque<std::vector<char>> pool;
    mutable std::mutex            pool_mutex;
    std::atomic<size_t>           counter;

    StringPool();
    StringPool(StringPool const&) = delete;
    StringPool& operator=(StringPool const&) = delete;

    /**
     * @brief add value to string pool
     * @param begin is a pointer to the begining of the string
     * @param end is a pointer to the next character after the end of the string
     * @return Z-order encoded address of the string (0 in case of error)
     */
    u64 add(const char* begin, const char* end);

    /**
     * @brief str returns string representation
     * @param bits is a Z-order encoded position in the string buffer
     * @return 0-copy string representation (or empty string)
     */
    StringT str(u64 bits) const;

    //! Get number of stored strings atomically
    size_t size() const;

    size_t mem_used() const;
};


struct StringTools {
    //! Pooled string
    typedef std::pair<const char*, int> StringT;

    static size_t hash(StringT str);
    static bool equal(StringT lhs, StringT rhs);

    typedef std::unordered_map<StringT, u64, decltype(&StringTools::hash),
                               decltype(&StringTools::equal)>
        TableT;

    typedef std::unordered_set<StringT, decltype(&StringTools::hash), decltype(&StringTools::equal)>
        SetT;

    typedef std::unordered_map<StringT, SetT, decltype(&StringTools::hash), decltype(&StringTools::equal)> L2TableT;

    typedef std::unordered_map<StringT, L2TableT, decltype(&StringTools::hash), decltype(&StringTools::equal)> L3TableT;

    //! Inverted table type (id to string mapping)
    typedef std::unordered_map<u64, StringT> InvT;

    static TableT create_table(size_t size);

    static SetT create_set(size_t size);

    static L2TableT create_l2_table(size_t size_hint);

    static L3TableT create_l3_table(size_t size_hint);
};
}
