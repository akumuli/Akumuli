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

        //! Close stream
        bool close();

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
            assert(value >= prev_);  // delta encoding must be used for sorted sequences
            auto delta = value - prev_;
            auto result = stream_.put(delta);
            prev_ = value;
            return result;
        }

        size_t size() const {
            return stream_.size();
        }

        bool close() {
            return stream_.close();
        }
    };


    template<class Stream, typename TVal>
    struct DeltaStreamReader {
        Stream& stream_;
        TVal prev_;

        DeltaStreamReader(Stream& stream)
            : stream_(stream)
            , prev_()
        {
        }

        void next(TVal* ret) {
            TVal delta;
            stream_.next(&delta);
            TVal value = prev_ + delta;
            prev_ = value;
            *ret = value;
        }
    };


    template<class Stream, typename TVal>
    struct RLEStreamWriter {
        Stream& stream_;
        TVal prev_;
        TVal reps_;

        RLEStreamWriter(Stream& stream)
            : stream_(stream)
            , prev_()
            , reps_()
        {}

        bool put(TVal value) {
            bool result = true;
            if (value != prev_) {
                if (reps_) {
                    // commit changes
                    result = stream_.put(reps_);
                    if (result) {
                        result = stream_.put(prev_);
                    }
                }
                prev_ = value;
                reps_ = TVal();
            }
            reps_++;
            return result;
        }

        size_t size() const {
            return stream_.size();
        }

        bool close() {
            bool result = true;
            result = stream_.put(reps_);
            if (result) {
                result = stream_.put(prev_);
            }
            return result;
        }
    };

    template<class Stream, typename TVal>
    struct RLEStreamReader {
        Stream& stream_;
        TVal prev_;
        TVal reps_;

        RLEStreamReader(Stream& stream)
            : stream_(stream)
            , prev_()
            , reps_()
        {}

        void next(TVal* out) {
            if (reps_ == 0) {
                stream_.next(&reps_);
                stream_.next(&prev_);
            }
            reps_--;
            *out = prev_;
        }
    };
}
