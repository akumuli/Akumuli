#pragma once

#include "util.h"

#include <boost/circular_buffer.hpp>
#include <boost/range.hpp>


namespace Akumuli {
namespace SAX {

struct SAXWord {

    // Compression schema
    // 0 - no data
    // 10 - 2 bits
    // 110 - 6 bits
    // 1110 - E bits
    // 11110 - 1E bits
    // 111110 - error

    uint32_t buffer[8];  // 32 bytes
    // TODO: use dynamic memory if word is too large

    /** C-tor.
     */
    SAXWord() : buffer{0}
    {
    }

    template<class FwdIt>
    SAXWord(FwdIt begin, FwdIt end)
        : SAXWord()
    {
        int ix = 0;
        int shift = 0;
        uint32_t* pbuf = buffer;
        for(auto item: boost::make_iterator_range(begin, end)) {
            // encode item
            char c = *item;
            int zerobits = leading_zeroes((int)c);
            int signbits = 8*sizeof(int) - zerobits;
            // Store mask
            if (signbits == 0) {
                // just update indexes
                shift++;
            } else {
                int nmask = 0;
                if (signbits < 3) {
                    nmask    = 2;
                    signbits = 2;
                } else if (signbits < 7) {
                    nmask    = 3;
                    signbits = 6;
                } else if (signbits < 0xF) {
                    nmask    = 4;
                    signbits = 30;
                } else if (signbits < 0x1E) {
                    nmask    = 5;
                    signbits = 0x1E;
                }
                for (int i = 0; i < nmask; i++) {
                    if (shift == 32) {
                        ix++;
                        shift = 0;
                    }
                    buffer[ix] |= ((1 & (signbits >> i)) << shift);
                    shift++;
                }
            }
            // Store payload
            for (int i = 0; i < signbits; i++) {
                if (shift == 32) {
                    ix++;
                    shift = 0;
                }
                buffer[ix] |= ((1 & (c >> i)) << shift);
                shift++;
            }
        }
    }
};

//! Symbolic Aggregate approXimmation encoder.
struct SAXEncoder
{
    const int alphabet_;      //! alphabet size
    const int window_width_;  //! sliding window width

    boost::circular_buffer<double> input_samples_;
    boost::circular_buffer<SAXWord>   output_samples_;

    /** C-tor
     * @param alphabet size should be a power of two
     */
    SAXEncoder(int alphabet, int window_width);

    /** Add sample to sliding window
     * @param sample value
     */
    void append(double sample);
};

}
}

