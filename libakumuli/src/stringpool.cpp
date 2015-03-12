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

#include "stringpool.h"

namespace Akumuli {

//                       //
//      String Pool      //
//                       //

StringPool::StringT StringPool::add(const char* begin, const char* end) {
    if (pool.empty()) {
        pool.emplace_back();
        pool.back().reserve(MAX_BIN_SIZE);
    }
    int size = end - begin;
    if (size == 0) {
        return std::make_pair("", 0);
    }
    size++;
    std::vector<char>* bin = &pool.back();
    if (static_cast<int>(bin->size()) + size > MAX_BIN_SIZE) {
        // New bin
        pool.emplace_back();
        bin = &pool.back();
        bin->reserve(MAX_BIN_SIZE);
    }
    for(auto i = begin; i < end; i++) {
        bin->push_back(*i);
    }
    bin->push_back('\0');
    const char* p = &bin->back();
    p -= size - 1;
    return std::make_pair(p, size-1);
}

size_t StringTools::hash(StringT str) {
    // implementation of Dan Bernstein's djb2
    const char* begin = str.first;
    int len = str.second;
    const char* end = begin + len;
    size_t hash = 5381;
    int c;
    while (begin < end) {
        c = *begin++;
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

bool StringTools::equal(StringT lhs, StringT rhs) {
    if (lhs.second != rhs.second) {
        return false;
    }
    return std::equal(lhs.first, lhs.first + lhs.second, rhs.first);
}

StringTools::TableT StringTools::create_table(size_t size) {
    return TableT(size, &StringTools::hash, &StringTools::equal);
}

}

