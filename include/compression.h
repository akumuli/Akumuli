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
#include <bits/stl_iterator.h>
#include <iterator>
#include <vector>

#include "akumuli.h"

namespace Akumuli {

    //! Base 128 encoded integer
    template<class TVal>
    class Base128Int {
        TVal value_;
        typedef unsigned char byte_t;
        typedef byte_t* byte_ptr;
    public:

        Base128Int(TVal val) : value_(val) {
        }

        Base128Int() : value_() {
        }

        /** Read base 128 encoded integer from the binary stream
        *  FwdIter - forward iterator
        */
        template<class FwdIter>
        FwdIter get(FwdIter begin, FwdIter end) {
            assert(begin < end);

            auto acc = TVal();
            auto cnt = TVal();
            FwdIter p = begin;

            while (true) {
                auto i = static_cast<byte_t>(*p & 0x7F);
                acc |= i << cnt;
                if ((*p++ & 0x80) == 0) {
                    break;
                }
                cnt += 7;
            }
            value_ = acc;
            return p;
        }

        /** Write base 128 encoded integer to the binary stream.
        * @returns 'begin' on error, iterator to next free region otherwise
        */
        template<class FwdIter>
        FwdIter put(FwdIter begin, FwdIter end) const {
            if (begin >= end) {
                return begin;
            }

            TVal value = value_;
            FwdIter p = begin;

            while (true) {
                *p = value & 0x7F;
                value >>= 7;
                if (value != 0) {
                    *p++ |= 0x80;
                    if (p == end) {
                        return begin;
                    }
                } else {
                    p++;
                    break;
                }
            }
            return p;
        }

        //! Write base 128 encoded integer to the binary stream (unchecked)
        template<class Inserter>
        void put(Inserter& p) const {
            TVal value = value_;
            byte_t result = 0;

            while(true) {
                result = value & 0x7F;
                value >>= 7;
                if (value != 0) {
                    result |= 0x80;
                    *p++ = result;
                } else {
                    *p++ = result;
                    break;
                }
            }
        }

        //! turn into integer
        operator TVal() const {
            return value_;
        }
    };

    typedef std::vector<unsigned char> ByteVector;

    //! Base128 encoder
    template<class TVal>
    struct Base128StreamWriter {
        // underlying memory region
        ByteVector& data_;

        Base128StreamWriter(ByteVector& data) : data_(data) {}

        /** Put value into stream.
         */
        void put(TVal value) {
            Base128Int<TVal> val(value);
            auto it = std::back_inserter(data_);
            val.put(it);
        }

        //! Close stream
        void close() {}

        size_t size() const {
            return data_.size();
        }

        aku_MemRange get_memrange() const {
            // FIXME: check for overflow
            return { data_.data(), static_cast<uint32_t>(data_.size()) };
        }
    };

    //! Base128 decoder
    template<class TVal, class FwdIt>
    struct Base128StreamReader {
        FwdIt pos_;
        FwdIt end_;

        Base128StreamReader(FwdIt begin, FwdIt end)
            : pos_(begin)
            , end_(end)
        {
        }

        TVal next() {
            Base128Int<TVal> value;
            pos_ = value.get(pos_, end_);
            return static_cast<TVal>(value);
        }

        typedef FwdIt Iterator;

        Iterator pos() const {
            return pos_;
        }
    };

    template<class Stream, class TVal>
    struct ZigZagStreamWriter {
        Stream stream_;

        ZigZagStreamWriter(ByteVector& container)
                : stream_(container) {
        }
        void put(TVal value) {
            // TVal must be signed
            const int shift_width = sizeof(TVal)*8 - 1;
            auto res = (value << 1) ^ (value >> shift_width);
            stream_.put(res);  // implicit type cast
        }
        size_t size() const {
            return stream_.size();
        }
        void close() {
            stream_.close();
        }
        aku_MemRange get_memrange() const {
            return stream_.get_memrange();
        }
    };

    template<class Stream, class TVal>
    struct ZigZagStreamReader {
        Stream stream_;

        template<class FwdIt>
        ZigZagStreamReader(FwdIt begin, FwdIt end)
                : stream_(begin, end) {
        }

        TVal next() {
            auto n = stream_.next();
            return (n >> 1) ^ (-(n & 1));
        }
        typedef typename Stream::Iterator Iterator;
        Iterator pos() const {
            return stream_.pos();
        }
    };

    template<class Stream, typename TVal>
    struct DeltaStreamWriter {
        Stream stream_;
        TVal prev_;

        DeltaStreamWriter(ByteVector& container)
            : stream_(container)
            , prev_()
        {
        }

        template<class InVal>
        void put(InVal value) {
            stream_.put(static_cast<TVal>(value) - prev_);
            prev_ = value;
        }

        size_t size() const {
            return stream_.size();
        }

        void close() {
            stream_.close();
        }

        aku_MemRange get_memrange() const {
            return stream_.get_memrange();
        }
    };


    template<class Stream, typename TVal>
    struct DeltaStreamReader {
        Stream stream_;
        TVal prev_;

        template<class FwdIt>
        DeltaStreamReader(FwdIt begin, FwdIt end)
            : stream_(begin, end)
            , prev_()
        {
        }

        TVal next() {
            TVal delta = stream_.next();
            TVal value = prev_ + delta;
            prev_ = value;
            return value;
        }

        typedef typename Stream::Iterator Iterator;

        Iterator pos() const {
            return stream_.pos();
        }
    };


    template<class Stream, typename TVal>
    struct RLEStreamWriter {
        Stream stream_;
        TVal prev_;
        TVal reps_;

        RLEStreamWriter(ByteVector& container)
            : stream_(container)
            , prev_()
            , reps_()
        {}

        void put(TVal value) {
            if (value != prev_) {
                if (reps_) {
                    // commit changes
                    stream_.put(reps_);
                    stream_.put(prev_);
                }
                prev_ = value;
                reps_ = TVal();
            }
            reps_++;
        }

        size_t size() const {
            return stream_.size();
        }

        void close() {
            stream_.put(reps_);
            stream_.put(prev_);
        }

        aku_MemRange get_memrange() const {
            return stream_.get_memrange();
        }
    };

    template<class Stream, typename TVal>
    struct RLEStreamReader {
        Stream stream_;
        TVal prev_;
        TVal reps_;

        template<class FwdIt>
        RLEStreamReader(FwdIt begin, FwdIt end)
            : stream_(begin, end)
            , prev_()
            , reps_()
        {}

        TVal next() {
            if (reps_ == 0) {
                reps_ = stream_.next();
                prev_ = stream_.next();
            }
            reps_--;
            return prev_;
        }

        typedef typename Stream::Iterator Iterator;

        Iterator pos() const {
            return stream_.pos();
        }
    };
}
