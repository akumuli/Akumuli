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
#include <iterator>
#include <vector>

#include "akumuli.h"

namespace Akumuli {

typedef std::vector<unsigned char> ByteVector;

enum ChunkHeaderCellType {
    NOT_SET = 0,
    INT,
    FLOAT,
    BLOB,
};

struct HeaderCell {
    // Types
    struct blob_t {
            uint32_t offset;
            uint32_t length;
    };
    union value_t {
        int64_t      intval;
        double     floatval;
        blob_t      blobval;
    };
    // Data
    int      type;
    value_t value;
};

struct ChunkHeader {
    /** Index in `timestamps` and `paramids` arrays corresponds
      * to individual row. Each element of the `values` array corresponds to
      * specific column and row. Variable longest_row should contain
      * longest row length inside the header.
      */
    std::vector<aku_Timestamp>  timestamps;
    std::vector<aku_ParamId>    paramids;
    int                         longest_row;
    std::vector<HeaderCell>     table[AKU_MAX_COLUMNS];
};

struct ChunkWriter {

    virtual ~ChunkWriter() = default;

    /** Allocate space for new data. Return mem range or
      * empty range in a case of error.
      */
    virtual aku_MemRange allocate() = 0;

    //! Commit changes
    virtual aku_Status commit(size_t bytes_written) = 0;
};

struct CompressionUtil {

    /** Compress and write ChunkHeader to memory stream.
      * @param n_elements out parameter - number of written elements
      * @param ts_begin out parameter - first timestamp
      * @param ts_end out parameter - last timestamp
      * @param data ChunkHeader to compress
      */
    static
    aku_Status encode_chunk( uint32_t           *n_elements
                           , aku_Timestamp      *ts_begin
                           , aku_Timestamp      *ts_end
                           , ChunkWriter        *writer
                           , const ChunkHeader &data
                           );

    /** Decompress ChunkHeader.
      * @brief Decode part of the ChunkHeader structure depending on stage and steps values.
      * First goes list of timestamps, then all other values.
      * @param header out header
      * @param pbegin in - begining of the data, out - new begining of the data
      * @param end end of the data
      * @param stage current stage
      * @param steps number of stages to do
      * @param probe_length number of elements in header
      * @return current stage number
      */
    static
    int decode_chunk( ChunkHeader          *header
                    , const unsigned char **pbegin
                    , const unsigned char  *pend
                    , int                   stage
                    , int                   steps
                    , uint32_t              probe_length);

    /** Compress list of doubles.
      * @param input array of doubles
      * @param params array of parameter ids
      * @param buffer resulting byte array
      */
    static
    size_t compress_doubles(std::vector<double> const& input,
                            ByteVector *buffer);  // TODO: maybe I should use plain old buffer here

    /** Decompress list of doubles.
      * @param buffer input data
      * @param numbloks number of 4bit blocs inside buffer
      * @param params list of parameter ids
      * @param output resulting array
      */
    static
    void decompress_doubles(ByteVector& buffer,
                            size_t numblocks,
                            std::vector<double> *output);

    /** Convert from chunk order to time order.
      * @note in chunk order all data elements ordered by series id first and then by timestamp,
      * in time order everythin ordered by time first and by id second.
      */
    static bool convert_from_chunk_order(const ChunkHeader &header, ChunkHeader* out);

    /** Convert from time order to chunk order.
      * @note in chunk order all data elements ordered by series id first and then by timestamp,
      * in time order everythin ordered by time first and by id second.
      */
    static bool convert_from_time_order(const ChunkHeader &header, ChunkHeader* out);
};


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
      * FwdIter - forward iterator.
      */
    unsigned char* get(unsigned char* begin, const unsigned char* end) {
        assert(begin < end);

        auto acc = TVal();
        auto cnt = TVal();
        unsigned char* p = begin;

        while (true) {
            if (p == end) {
                return begin;
            }
            auto i = static_cast<byte_t>(*p & 0x7F);
            acc |= TVal(i) << cnt;
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
    unsigned char* put(unsigned char* begin, const unsigned char* end) const {
        if (begin >= end) {
            return begin;
        }

        TVal value = value_;
        unsigned char* p = begin;

        while (true) {
            if (p == end) {
                return begin;
            }
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

    //! turn into integer
    operator TVal() const {
        return value_;
    }
};

//! Base128 encoder
struct Base128StreamWriter {
    // underlying memory region
    const unsigned char* begin_;
    const unsigned char* end_;
    unsigned char* pos_;

    Base128StreamWriter(unsigned char* begin, const unsigned char* end) 
        : begin_(begin)
        , end_(end)
        , pos_(begin)
    {
    }

    Base128StreamWriter(Base128StreamWriter& other)
        : begin_(other.begin_)
        , end_(other.end_)
        , pos_(other.pos_)
    {
    }

    /** Put value into stream.
     */
    template<class TVal>
    aku_Status put(TVal value) {
        Base128Int<TVal> val(value);
        unsigned char* p = val.put(pos_, end_);
        if (pos_ == p) {
            return AKU_EOVERFLOW;
        }
        pos_ = p;
        return AKU_SUCCESS;
    }

    //! Commit stream
    void commit() {}

    size_t size() const {
        return pos_ - begin_;
    }

    size_t space_left() const {
        return end_ - pos_;
    }

    /** Try to allocate space inside a stream for future use (needed for size prefixes)
      * @returns null on error (no space)
      */
    template<class T>
    T* allocate() {
        size_t sz = sizeof(T);
        if (space_left() < sz) {
            return nullptr;
        }
        T* result = reinterpret_cast<T*>(pos_);
        pos_ += sz;
        return result;
    }
};

//! Base128 decoder
struct Base128StreamReader {
    unsigned char* pos_;
    const unsigned char* end_;

    Base128StreamReader(unsigned char* begin, const unsigned char* end)
        : pos_(begin)
        , end_(end)
    {
    }

    template<class TVal>
    TVal next() {
        Base128Int<TVal> value;
        auto p = value.get(pos_, end_);
        if (p == pos_) {
            // report error
        }
        pos_ = p;
        return static_cast<TVal>(value);
    }

    //! Read uncompressed value from stream
    template<class TVal>
    TVal* read_raw() {
        size_t sz = sizeof(TVal);
        if (space_left() < sz) {
            return nullptr;
        }
        TVal* val = *reinterpret_cast<TVal*>(pos_);
        pos_ += sz;
        return val;
    }

    size_t space_left() const {
        return end_ - pos_;
    }

    unsigned char* pos() const {
        return pos_;
    }
};

template<class Stream, class TVal>
struct ZigZagStreamWriter {
    Stream stream_;
    size_t start_size_;

    ZigZagStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , start_size_(stream.size())
    {
    }
    aku_Status put(TVal value) {
        // TVal should be signed
        const int shift_width = sizeof(TVal)*8 - 1;
        auto res = (value << 1) ^ (value >> shift_width);
        return stream_.put(res);
    }
    size_t size() const {
        return stream_.size() - start_size_;
    }
    void commit() {
        stream_.commit();
    }
};

template<class Stream, class TVal>
struct ZigZagStreamReader {
    Stream stream_;

    ZigZagStreamReader(Base128StreamReader& stream)
        : stream_(stream) {
    }

    TVal next() {
        auto n = stream_.next();
        return (n >> 1) ^ (-(n & 1));
    }

    unsigned char* pos() const {
        return stream_.pos();
    }
};

template<class Stream, typename TVal>
struct DeltaStreamWriter {
    Stream stream_;
    TVal prev_;
    size_t start_size_;

    DeltaStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_()
        , start_size_(stream.size())
    {
    }

    aku_Status put(TVal value) {
        auto status = stream_.put(static_cast<TVal>(value) - prev_);
        prev_ = value;
        return status;
    }

    size_t size() const {
        return stream_.size() - start_size_;
    }

    void commit() {
        stream_.commit();
    }
};


template<class Stream, typename TVal>
struct DeltaStreamReader {
    Stream stream_;
    TVal prev_;

    DeltaStreamReader(Base128StreamReader& stream)
        : stream_(stream)
        , prev_()
    {
    }

    TVal next() {
        TVal delta = stream_.next();
        TVal value = prev_ + delta;
        prev_ = value;
        return value;
    }

    unsigned char* pos() const {
        return stream_.pos();
    }
};


template<typename TVal>
struct RLEStreamWriter {
    Base128StreamWriter& stream_;
    TVal prev_;
    TVal reps_;
    size_t start_size_;

    RLEStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_()
        , reps_()
        , start_size_(stream.size())
    {}

    aku_Status put(TVal value) {
        aku_Status status = AKU_SUCCESS;
        if (value != prev_) {
            if (reps_) {
                // commit changes
                status = stream_.put(reps_);
                if (status != AKU_SUCCESS) {
                    goto END;
                }
                status = stream_.put(prev_);
                if (status != AKU_SUCCESS) {
                    goto END;
                }
            }
            prev_ = value;
            reps_ = TVal();
        }
        reps_++;
    END:
        return status;
    }

    size_t size() const {
        return stream_.size() - start_size_;
    }

    void commit() {
        stream_.put(reps_);
        stream_.put(prev_);
        stream_.commit();
    }
};

template<typename TVal>
struct RLEStreamReader {
    Base128StreamReader& stream_;
    TVal prev_;
    TVal reps_;

    RLEStreamReader(Base128StreamReader& stream)
        : stream_(stream)
        , prev_()
        , reps_()
    {}

    TVal next() {
        if (reps_ == 0) {
            reps_ = stream_.next<TVal>();
            prev_ = stream_.next<TVal>();
        }
        reps_--;
        return prev_;
    }

    unsigned char* pos() const {
        return stream_.pos();
    }
};

// Length -> RLE -> Base128
typedef RLEStreamWriter<uint32_t> RLELenWriter;

// Base128 -> RLE -> Length
typedef RLEStreamReader<uint32_t> RLELenReader;

// int64_t -> Delta -> ZigZag -> RLE -> Base128
typedef RLEStreamWriter<int64_t> __RLEWriter;
typedef ZigZagStreamWriter<__RLEWriter, int64_t> __ZigZagWriter;
typedef DeltaStreamWriter<__ZigZagWriter, int64_t> DeltaRLEWriter;

// Base128 -> RLE -> ZigZag -> Delta -> int64_t
typedef RLEStreamReader<int64_t> __RLEReader;
typedef ZigZagStreamReader<__RLEReader, int64_t> __ZigZagReader;
typedef DeltaStreamReader<__ZigZagReader, int64_t> DeltaRLEReader;

}
