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
 *
 */
#pragma once

#include <boost/circular_buffer.hpp>
#include <boost/range.hpp>


namespace Akumuli {
namespace SAX {

int leading_zeroes(int value);

struct SAXWord {

    // Compression schema
    // 0 - no data stored (zero symbol)
    // 10 - 2 bits
    // 110 - 6 bits
    // 1110 - E bits
    // 11110 - 1E bits
    // 111110 - error

    enum { SIZE = 16 };

    char buffer[SIZE];

    /** C-tor.
     */
    SAXWord()
        : buffer{ 0 } {}

    //! C-tor for unit-tests
    SAXWord(const char* str)
        : SAXWord(str, str + strlen(str)) {}

    //! Copy c-tor
    SAXWord(const SAXWord& other) { memcpy(buffer, other.buffer, SIZE); }

    SAXWord& operator=(const SAXWord& other) {
        if (&other != this) {
            memcpy(buffer, other.buffer, SIZE);
        }
        return *this;
    }

    bool operator!=(const SAXWord& other) const {
        return !std::equal(buffer, buffer + SIZE, other.buffer);
    }

    bool operator==(const SAXWord& other) const {
        return std::equal(buffer, buffer + SIZE, other.buffer);
    }

    //! Copy data from sequence
    template <class FwdIt>
    SAXWord(FwdIt begin, FwdIt end)
        : SAXWord() {
        int ix    = 0;
        int shift = 0;
        for (auto payload : boost::make_iterator_range(begin, end)) {
            int zerobits = leading_zeroes((int)payload);
            int signbits = 8 * sizeof(int) - zerobits;
            // Store mask
            if (signbits == 0) {
                // just update indexes
                shift++;
            } else {
                int mask  = 0;
                int nmask = 0;  // number of bits in mask
                if (signbits < 3) {
                    mask     = 2;
                    nmask    = 2;
                    signbits = 2;
                } else if (signbits < 7) {
                    mask     = 6;
                    nmask    = 3;
                    signbits = 6;
                } else if (signbits < 0xF) {
                    mask     = 0xE;
                    nmask    = 4;
                    signbits = 0xE;
                } else if (signbits < 0x1E) {
                    mask     = 0x1E;
                    nmask    = 5;
                    signbits = 0x1E;
                }
                for (int i = nmask; i-- > 0;) {
                    if (shift == 8) {
                        ix++;
                        shift = 0;
                        if (ix == SIZE) {
                            std::runtime_error error("SAX word too long");
                            BOOST_THROW_EXCEPTION(error);
                        }
                    }
                    buffer[ix] |= ((1 & (mask >> i)) << shift);
                    shift++;
                }
            }
            // Store payload
            for (int i = signbits; i-- > 0;) {
                if (shift == 8) {
                    ix++;
                    shift = 0;
                    if (ix == SIZE) {
                        std::runtime_error error("SAX word too long");
                        BOOST_THROW_EXCEPTION(error);
                    }
                }
                buffer[ix] |= ((1 & (payload >> i)) << shift);
                shift++;
            }
        }
    }

    template <class It> void read_n(int N, It it) const {
        int  ix           = 0;
        int  shift        = 0;
        int  mask         = 0;
        int  nbits        = 0;
        bool read_payload = false;
        for (int i = 0; i < N;) {
            mask <<= 1;
            mask |= (buffer[ix] >> shift) & 0x1;
            shift++;
            if (shift == 8) {
                ix++;
                shift = 0;
                if (ix == SIZE) {
                    std::runtime_error error("sax word decoding out of bounds");
                    BOOST_THROW_EXCEPTION(error);
                }
            }
            switch (mask) {
            case 0:
                read_payload = true;
                nbits        = 0;
                break;
            case 2:
                read_payload = true;
                nbits        = 2;
                break;
            case 6:
                read_payload = true;
                nbits        = 6;
                break;
            case 0xE:
                read_payload = true;
                nbits        = 0xE;
                break;
            case 0x1E:
                read_payload = true;
                nbits        = 0x1E;
                break;
            default:
                if (mask > 0x1E) {
                    std::runtime_error error("invalid SAX word encoding");
                    BOOST_THROW_EXCEPTION(error);
                }
                break;
            }
            if (read_payload) {
                int payload = 0;
                for (int j = 0; j < nbits; j++) {
                    payload <<= 1;
                    payload |= (buffer[ix] >> shift) & 0x1;
                    shift++;
                    if (shift == 8) {
                        ix++;
                        shift = 0;
                        if (ix == SIZE) {
                            std::runtime_error error("sax word decoding out of bounds");
                            BOOST_THROW_EXCEPTION(error);
                        }
                    }
                }
                *it++        = payload;
                read_payload = false;
                mask         = 0;
                nbits        = 0;
                i++;
            }
        }
    }
};


//! Symbolic Aggregate approXimmation encoder.
struct SAXEncoder {
    int alphabet_;      //! alphabet size
    int window_width_;  //! sliding window width

    boost::circular_buffer<double> input_samples_;
    std::string                    buffer_;
    std::string                    last_;

    SAXEncoder();

    /** C-tor
     * @param alphabet size should be a power of two
     */
    SAXEncoder(int alphabet, int window_width);

    /** Add sample to sliding window
     * @param sample value
     * @param outword receives new sax word if true returned
     * @returns true if new sax word returned; false otherwise
     */
    bool encode(double sample, char* outword, size_t outword_size);
};
}
}
