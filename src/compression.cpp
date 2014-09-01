#include "compression.h"

namespace Akumuli {

    //! Base 128 encoded integer
    class Base128Int {
        uint64_t value_;
        typedef unsigned char byte_t;
        typedef byte_t* byte_ptr;
    public:

        Base128Int(uint64_t val) : value_(val) {
        }

        Base128Int() : value_(0ul) {
        }

        /** Read base 128 encoded integer from the binary stream
         *  FwdIter - forward iterator
         */
        template<class FwdIter> 
        FwdIter get(FwdIter begin, FwdIter end) {
            assert(begin < end);
    
            uint64_t acc = 0ul;
            uint64_t cnt = 0ul;
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
    
            uint64_t value = value_;
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

        //! turn into integer
        operator uint64_t() const {
            return value_;
        }
    };


    // Base128StreamWriter

    Base128StreamWriter::Base128StreamWriter(unsigned char* ptr, const size_t size) 
        : begin_(ptr)
        , end_(ptr + size)
        , pos_(ptr)
    {
    }

    Base128StreamWriter::Base128StreamWriter(unsigned char* begin, unsigned char* end) 
        : begin_(begin)
        , end_(end)
        , pos_(begin)
    {
    }

    bool Base128StreamWriter::put(uint64_t value) {
        Base128Int val(value);
        auto old_pos = pos_;
        pos_ = val.put(pos_, end_);
        return pos_ != old_pos;
    }

    size_t Base128StreamWriter::size() const {
        return pos_ - begin_;
    }


    Base128StreamReader::Base128StreamReader(unsigned char* ptr, const size_t size) 
        : begin_(ptr)
        , end_(ptr + size)
        , pos_(ptr)
    {
    }

    Base128StreamReader::Base128StreamReader(unsigned char* begin, unsigned char* end)
        : begin_(begin)
        , end_(end)
        , pos_(begin)
    {
    }

    uint64_t Base128StreamReader::next() {
        Base128Int value;
        pos_ = value.get(pos_, end_);
        return static_cast<uint64_t>(value);
    }

}