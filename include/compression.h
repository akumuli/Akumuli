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

        /** Put value into stream.
        * @returns true on success, false if on overflow
        */
        bool put(uint32_t value);

        size_t size() const;
    };

    //! Base128 decoder
    struct Base128StreamReader {
        unsigned char
            *begin_, *end_, *pos_;

        Base128StreamReader(unsigned char* ptr, const size_t size);

        Base128StreamReader(unsigned char* begin, unsigned char* end);

        void next(uint64_t* value);

        void next(uint32_t* value);
    };


    template<class Stream, typename TVal>
    struct DeltaStreamWriter {
        Stream& stream_;
        TVal prev_;


        DeltaStreamWriter(Stream& stream)
            : stream_(stream)
            , prev_()
        {
        }

        bool put(TVal value) {
            auto delta = value - prev_;
            stream_.put(delta);
            prev_ = value;
        }

        size_t size() const {
            return stream_.size();
        }
    };


    template<class Stream, typename TVal>
    struct DeltaStreamReader {
        Stream const& stream_;
        TVal prev_;

        DeltaStreamReader(Stream const& stream)
            : stream_(stream)
        {
        }

        void next(TVal* ret) {
            auto delta = stream_.next();
            TVal value = prev_ + delta;
            prev_ = value;
            *ret = value;
        }
    };


    template<class Stream, typename TVal>
    struct RLEStreamWriter {
    };
}
