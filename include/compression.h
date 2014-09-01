/**
 * PRIVATE HEADER
 *
 * Compression algorithms
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
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

#include <cassert>
#include <cstdint>
#include <cstddef>

namespace Akumuli {

    //! Base128 encoder
    struct Base128StreamWriter {
        // underlying memory region
        unsigned char 
            *begin_, *end_, *pos_;

        Base128StreamWriter(unsigned char* ptr, const size_t size);

        Base128StreamWriter(unsigned char* begin, unsigned char* end);

        /** Put value into stream.
         * @returns true on success, false if on overflow
         */
        bool put(uint64_t value);

        size_t size() const;
    };

    //! Base128 decoder
    struct Base128StreamReader {
        unsigned char
            *begin_, *end_, *pos_;

        Base128StreamReader(unsigned char* ptr, const size_t size);

        Base128StreamReader(unsigned char* begin, unsigned char* end);

        uint64_t next();
    };
}
