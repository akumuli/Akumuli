#pragma once

#include <boost/circular_buffer.hpp>
#include <boost/range.hpp>


namespace Akumuli {
namespace SAX {

int leading_zeroes(int value);

struct SAXWord {

    // Compression schema
    // 0 - no data
    // 10 - 2 bits
    // 110 - 6 bits
    // 1110 - E bits
    // 11110 - 1E bits
    // 111110 - error

    char buffer[32];
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
        for(auto payload: boost::make_iterator_range(begin, end)) {
            int zerobits = leading_zeroes((int)payload);
            int signbits = 8*sizeof(int) - zerobits;
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
                for (int i = nmask; i --> 0;) {
                    if (shift == 8) {
                        ix++;
                        shift = 0;
                    }
                    buffer[ix] |= ((1 & (mask >> i)) << shift);
                    shift++;
                }
            }
            // Store payload
            for (int i = signbits; i --> 0;) {
                if (shift == 8) {
                    ix++;
                    shift = 0;
                }
                buffer[ix] |= ((1 & (payload >> i)) << shift);
                shift++;
            }
        }
    }

    template<class It>
    void read_n(int N, It it) {
        int ix = 0;
        int shift = 0;
        int mask = 0;
        int nbits = 0;
        bool read_payload = false;
        for (int i = 0; i < N;) {
            mask <<= 1;
            mask |= (buffer[ix] >> shift) & 0x1;
            shift++;
            if (shift == 8) {
                ix++;
                shift = 0;
            }
            switch(mask) {
            case 0:
                read_payload = true;
                nbits = 0;
                break;
            case 2:
                read_payload = true;
                nbits = 2;
                break;
            case 6:
                read_payload = true;
                nbits = 6;
                break;
            case 0xE:
                read_payload = true;
                nbits = 0xE;
                break;
            case 0x1E:
                read_payload = true;
                nbits = 0x1E;
                break;
            default:
                break;
            }
            if (read_payload) {
                int payload = 0;
                for(int j = 0; j < nbits; j++) {
                    payload <<= 1;
                    payload |= (buffer[ix] >> shift) & 0x1;
                    shift++;
                    if (shift == 8) {
                        ix++;
                        shift = 0;
                    }
                }
                *it++ = payload;
                read_payload = false;
                mask = 0;
                nbits = 0;
                i++;
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

