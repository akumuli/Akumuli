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
#include <cinttypes>

namespace Akumuli {

    //! Base 128 encoded integer
    class Base128Int {
        uint64_t value_;
        typedef unsigned char byte_t;
        typedef byte_t* byte_ptr;
    public:

        Base128Int(uint64_t val);
        Base128Int();

        /** Read base 128 encoded integer from the binary stream
         *  FwdIter - forward iterator
         */
        template<class FwdIter> 
        FwdIter get(FwdIter begin, FwdIter end) {
            assert(begin < end);
    
            uint64_t acc = 0ul;
            uint64_t cnt = 0ul;
            FwdIter p = begin;
    
            while(true) {
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

        //! Write base 128 encoded integer to the binary stream
        template<class FwdIter> 
        FwdIter put(FwdIter begin, FwdIter end) const {
            assert(begin < end);
    
            uint64_t value = value_;
            FwdIter p = begin;
    
            while(true) {
                *p = value & 0x7F;
                value >>= 7;
                if (value != 0) {
                    *p++ |= 0x80;
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
            uint64_t value = value_;
            byte_t result = 0u;
    
            while(true) {
                result = value & 0x7F;
                value >>= 7;
                if (value != 0u)
                {
                    result |= 0x80;
                    *p++ = result;
                }
                else
                {
                    *p++ = result;
                    break;
                }
            }
        }

        //! turn into integer
        operator uint64_t() const;
    };

}
