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
#include <boost/regex.hpp>

namespace Akumuli {

//                       //
//      String Pool      //
//                       //

StringPool::StringT StringPool::add(const char* begin, const char* end, uint64_t payload) {
    std::lock_guard<std::mutex> guard(pool_mutex);  // Maybe I'll need to optimize this
    if (pool.empty()) {
        pool.emplace_back();
        pool.back().reserve(MAX_BIN_SIZE);
    }
    int size = end - begin;
    if (size == 0) {
        return std::make_pair("", 0);
    }
    size += 2 + sizeof(uint64_t);  // 2 is for two \0 characters
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
    for(int i = 56; i >= 0; i -= 8) {
        bin->push_back(payload >> i);
    }
    bin->push_back('\0');
    const char* p = &bin->back();
    p -= size - 1;
    int token_size = end - begin;
    return std::make_pair(p, token_size);
}

std::vector<StringPool::StringT> StringPool::regex_match(const char *regex) const {
    std::vector<StringPool::StringT> results;
    boost::regex series_regex(regex, boost::regex_constants::optimize);
    typedef std::vector<char> const* PBuffer;
    std::vector<PBuffer> buffers;
    {
        std::lock_guard<std::mutex> guard(pool_mutex);
        for(auto& buf: pool) {
            buffers.push_back(&buf);
        }
    }
    for(auto pbuf: buffers) {
        auto begin = boost::cregex_iterator(pbuf->data(), pbuf->data() + pbuf->size(), series_regex);
        auto end = boost::cregex_iterator();
        for(boost::cregex_iterator i = begin; i != end; i++) {
            boost::cmatch match = *i;
            const char* p = match[0].first;
            size_t sz = match[0].second - match[0].first;
            results.push_back(std::make_pair(p, sz));
        }
    }
    return results;
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

