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
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <stdexcept>
#include <vector>

#include "akumuli.h"
#include "util.h"

namespace Akumuli {

typedef std::vector<unsigned char> ByteVector;

struct UncompressedChunk {
    /** Index in `timestamps` and `paramids` arrays corresponds
      * to individual row. Each element of the `values` array corresponds to
      * specific column and row. Variable longest_row should contain
      * longest row length inside the header.
      */
    std::vector<aku_Timestamp> timestamps;
    std::vector<aku_ParamId>   paramids;
    std::vector<double>        values;
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

//! Base 128 encoded integer
template <class TVal> class Base128Int {
    TVal                  value_;
    typedef unsigned char byte_t;
    typedef byte_t*       byte_ptr;

public:
    Base128Int(TVal val)
        : value_(val) {}

    Base128Int()
        : value_() {}

    /** Read base 128 encoded integer from the binary stream
      * FwdIter - forward iterator.
      */
    const unsigned char* get(const unsigned char* begin, const unsigned char* end) {
        assert(begin < end);

        auto                 acc = TVal();
        auto                 cnt = TVal();
        const unsigned char* p   = begin;

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

        TVal           value = value_;
        unsigned char* p     = begin;

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
    operator TVal() const { return value_; }
};

//! Base128 encoder
struct Base128StreamWriter {
    // underlying memory region
    const unsigned char* begin_;
    const unsigned char* end_;
    unsigned char*       pos_;

    Base128StreamWriter(unsigned char* begin, const unsigned char* end)
        : begin_(begin)
        , end_(end)
        , pos_(begin) {}

    Base128StreamWriter(Base128StreamWriter& other)
        : begin_(other.begin_)
        , end_(other.end_)
        , pos_(other.pos_) {}

    /** Put value into stream (transactional).
      */
    template <class TVal>
    bool tput(TVal const* iter, size_t n) {
        auto oldpos = pos_;
        for (size_t i = 0; i < n; i++) {
            if (!put(iter[i])) {
                // restore old pos_ value
                pos_ = oldpos;
                return false;
            }
        }
        return commit();  // no-op
    }

    /** Put value into stream.
     */
    template <class TVal>
    bool put(TVal value) {
        Base128Int<TVal> val(value);
        unsigned char*   p = val.put(pos_, end_);
        if (pos_ == p) {
            return false;
        }
        pos_ = p;
        return true;
    }

    bool put_raw(unsigned char value) {
        if (pos_ == end_) {
            return false;
        }
        *pos_++ = value;
        return true;
    }

    bool put_raw(uint32_t value) {
        if ((end_ - pos_) < (int)sizeof(value)) {
            return false;
        }
        *reinterpret_cast<uint32_t*>(pos_) = value;
        pos_ += sizeof(value);
        return true;
    }

    bool put_raw(uint64_t value) {
        if ((end_ - pos_) < (int)sizeof(value)) {
            return false;
        }
        *reinterpret_cast<uint64_t*>(pos_) = value;
        pos_ += sizeof(value);
        return true;
    }


    //! Commit stream
    bool commit() { return true; }

    size_t size() const { return pos_ - begin_; }

    size_t space_left() const { return end_ - pos_; }

    /** Try to allocate space inside a stream in current position without
      * compression (needed for size prefixes).
      * @returns pointer to the value inside the stream or nullptr
      */
    template <class T>
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
    const unsigned char* pos_;
    const unsigned char* end_;

    Base128StreamReader(const unsigned char* begin, const unsigned char* end)
        : pos_(begin)
        , end_(end)
    {}

    template <class TVal> TVal next() {
        Base128Int<TVal> value;
        auto             p = value.get(pos_, end_);
        if (p == pos_) {
            AKU_PANIC("can't read value, out of bounds");
        }
        pos_ = p;
        return static_cast<TVal>(value);
    }

    //! Read uncompressed value from stream
    template <class TVal> TVal read_raw() {
        size_t sz = sizeof(TVal);
        if (space_left() < sz) {
            AKU_PANIC("can't read value, out of bounds");
        }
        auto val = *reinterpret_cast<const TVal*>(pos_);
        pos_ += sz;
        return val;
    }

    size_t space_left() const { return end_ - pos_; }

    const unsigned char* pos() const { return pos_; }
};


template <class Stream, class TVal>
struct ZigZagStreamWriter {
    Stream stream_;

    ZigZagStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
    {}

    bool tput(TVal const* iter, size_t n) {
        TVal outbuf[n];
        for (size_t i = 0; i < n; i++) {
            auto      value       = iter[i];
            const int shift_width = sizeof(TVal) * 8 - 1;
            auto      res         = (value << 1) ^ (value >> shift_width);
            outbuf[i]             = res;
        }
        return stream_.tput(outbuf, n);
    }

    bool put(TVal value) {
        // TVal should be signed
        const int shift_width = sizeof(TVal) * 8 - 1;
        auto      res         = (value << 1) ^ (value >> shift_width);
        return stream_.put(res);
    }

    size_t size() const { return stream_.size(); }

    bool commit() { return stream_.commit(); }
};

template <class Stream, class TVal> struct ZigZagStreamReader {
    Stream stream_;

    ZigZagStreamReader(Base128StreamReader& stream)
        : stream_(stream) {}

    TVal next() {
        auto n = stream_.next();
        return (n >> 1) ^ (-(n & 1));
    }

    const unsigned char* pos() const { return stream_.pos(); }
};

template <class Stream, typename TVal>
struct DeltaStreamWriter {
    Stream stream_;
    TVal   prev_;

    DeltaStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_() {}

    bool tput(TVal const* iter, size_t n) {
        TVal outbuf[n];
        for (size_t i = 0; i < n; i++) {
            auto value  = iter[i];
            auto result = static_cast<TVal>(value) - prev_;
            outbuf[i]   = result;
            prev_       = value;
        }
        return stream_.tput(outbuf, n);
    }

    bool put(TVal value) {
        auto result = stream_.put(static_cast<TVal>(value) - prev_);
        prev_ = value;
        return result;
    }

    size_t size() const { return stream_.size(); }

    bool commit() { return stream_.commit(); }
};


template <class Stream, typename TVal>
struct DeltaStreamReader {
    Stream stream_;
    TVal   prev_;

    DeltaStreamReader(Base128StreamReader& stream)
        : stream_(stream)
        , prev_() {}

    TVal next() {
        TVal delta = stream_.next();
        TVal value = prev_ + delta;
        prev_      = value;
        return value;
    }

    const unsigned char* pos() const { return stream_.pos(); }
};


template <size_t Step, class Stream, typename TVal>
struct DeltaDeltaStreamWriter {
    Stream stream_;
    TVal   prev_;

    DeltaDeltaStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_() {}

    bool tput(TVal const* iter, size_t n) {
        assert(n == Step);
        TVal outbuf[n];
        for (size_t i = 0; i < n; i++) {
            auto value  = iter[i];
            auto result = static_cast<TVal>(value) - prev_;
            outbuf[i]   = result;
            prev_       = value;
        }
        TVal min = outbuf[0];
        for (size_t i = 1; i < n; i++) {
            min = std::min(outbuf[i], min);
        }
        for (size_t i = 0; i < n; i++) {
            outbuf[i] -= min;
        }
        // encode min value
        if (!stream_.put(min)) {
            return false;
        }
        return stream_.tput(outbuf, n);
    }

    bool put(TVal value) {
        // TODO: this wouldn't work with delta-delta encoding
        // we should put min=0 to the underlying stream first
        // and then we can use simple `put` method to write
        // values (less then Step times).
        auto result = stream_.put(static_cast<TVal>(value) - prev_);
        prev_ = value;
        return result;
    }

    size_t size() const { return stream_.size(); }

    bool commit() { return stream_.commit(); }
};


template <typename TVal>
struct RLEStreamWriter {
    Base128StreamWriter& stream_;
    TVal                 prev_;
    TVal                 reps_;
    size_t               start_size_;

    RLEStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_()
        , reps_()
        , start_size_(stream.size()) {}

    bool tput(TVal const* iter, size_t  n) {
        size_t outpos = 0;
        TVal outbuf[n*2];
        for (size_t i = 0; i < n; i++) {
            auto value = iter[i];
            if (value != prev_) {
                if (reps_) {
                    // commit changes
                    outbuf[outpos++] = reps_;
                    outbuf[outpos++] = prev_;
                }
                prev_ = value;
                reps_ = TVal();
            }
            reps_++;
        }
        // commit RLE if needed
        if (outpos < n*2) {
            outbuf[outpos++] = reps_;
            outbuf[outpos++] = prev_;
        }
        prev_ = TVal();
        reps_ = TVal();
        // continue
        return stream_.tput(outbuf, outpos);
    }

    bool put(TVal value) {
        //
        if (value != prev_) {
            if (reps_) {
                // commit changes
                if (!stream_.put(reps_)) {
                    return false;
                }
                if (!stream_.put(prev_)) {
                    return false;
                }
            }
            prev_ = value;
            reps_ = TVal();
        }
        reps_++;
        return true;
    }

    size_t size() const { return stream_.size() - start_size_; }

    bool commit() {
        return stream_.put(reps_) && stream_.put(prev_) && stream_.commit();
    }
};

template <typename TVal>
struct RLEStreamReader {
    Base128StreamReader& stream_;
    TVal                 prev_;
    TVal                 reps_;

    RLEStreamReader(Base128StreamReader& stream)
        : stream_(stream)
        , prev_()
        , reps_() {}

    TVal next() {
        if (reps_ == 0) {
            reps_ = stream_.next<TVal>();
            prev_ = stream_.next<TVal>();
        }
        reps_--;
        return prev_;
    }

    const unsigned char* pos() const { return stream_.pos(); }
};

struct FcmPredictor {
    std::vector<uint64_t> table;
    uint64_t last_hash;
    const uint64_t MASK_;

    FcmPredictor(size_t table_size);

    uint64_t predict_next() const;

    void update(uint64_t value);
};

struct DfcmPredictor {
    std::vector<uint64_t> table;
    uint64_t last_hash;
    uint64_t last_value;
    const uint64_t MASK_;

    //! C-tor. `table_size` should be a power of two.
    DfcmPredictor(int table_size);

    uint64_t predict_next() const;

    void update(uint64_t value);
};

typedef FcmPredictor PredictorT;

struct FcmStreamWriter {
    Base128StreamWriter& stream_;
    PredictorT predictor_;
    uint64_t prev_diff_;
    unsigned char prev_flag_;
    int nelements_;

    FcmStreamWriter(Base128StreamWriter& stream);

    bool tput(double const* values, size_t n);

    bool put(double value);

    size_t size() const;

    bool commit();
};

struct FcmStreamReader {
    Base128StreamReader& stream_;
    PredictorT predictor_;
    int flags_;
    int iter_;

    FcmStreamReader(Base128StreamReader& stream);

    double next();

    const unsigned char* pos() const;
};


//! SeriesSlice represents consiquent data points from one series
struct SeriesSlice {
    //! Series id
    aku_ParamId id;
    //! Pointer to the array of timestamps
    aku_Timestamp* ts;
    //! Pointer to the array of values
    double* value;
    //! Array size
    size_t size;
    //! Current position
    size_t offset;
};

struct CompressionUtil {

    /** Compress and write slice into buffer
      * @param slice is a structure representing time-series state
      * @param buffer is a pointer to destination buffer
      * @param buffer_size is a buffer size
      */
    static aku_Status encode_block(SeriesSlice *slice, uint8_t* buffer, size_t buffer_size);

    /** Return number of elements stored in the block
      */
    static uint32_t number_of_elements_in_block(uint8_t const* buffer, size_t buffer_size);

    /** Decode block and write result into slice
      * @param buffer is a pointer to buffer that stores encoded data
      * @param buffer_size is a size of the buffer
      * @param dest is a pointer to slice that should receive all results
      */
    static aku_Status decode_block(uint8_t const* buffer, size_t buffer_size, SeriesSlice* dest);


    // Old depricated functions

    /** Compress and write ChunkHeader to memory stream.
      * @param n_elements out parameter - number of written elements
      * @param ts_begin out parameter - first timestamp
      * @param ts_end out parameter - last timestamp
      * @param data ChunkHeader to compress
      */
    static aku_Status encode_chunk(uint32_t* n_elements, aku_Timestamp* ts_begin,
                                   aku_Timestamp* ts_end, ChunkWriter* writer,
                                   const UncompressedChunk& data);

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
    static aku_Status decode_chunk(UncompressedChunk* header, const unsigned char* pbegin,
                                   const unsigned char* pend, uint32_t nelements);

    /** Compress list of doubles.
      * @param input array of doubles
      * @param params array of parameter ids
      * @param buffer resulting byte array
      */
    static size_t compress_doubles(const std::vector<double>& input, Base128StreamWriter& wstream);

    /** Decompress list of doubles.
      * @param buffer input data
      * @param numbloks number of 4bit blocs inside buffer
      * @param params list of parameter ids
      * @param output resulting array
      */
    static void decompress_doubles(Base128StreamReader& rstream, size_t numvalues,
                                   std::vector<double>* output);

    /** Convert from chunk order to time order.
      * @note in chunk order all data elements ordered by series id first and then by timestamp,
      * in time order everythin ordered by time first and by id second.
      */
    static bool convert_from_chunk_order(const UncompressedChunk& header, UncompressedChunk* out);

    /** Convert from time order to chunk order.
      * @note in chunk order all data elements ordered by series id first and then by timestamp,
      * in time order everythin ordered by time first and by id second.
      */
    static bool convert_from_time_order(const UncompressedChunk& header, UncompressedChunk* out);
};

// Length -> RLE -> Base128
typedef RLEStreamWriter<uint32_t> RLELenWriter;

// Base128 -> RLE -> Length
typedef RLEStreamReader<uint32_t> RLELenReader;

// int64_t -> Delta -> ZigZag -> RLE -> Base128
typedef RLEStreamWriter<int64_t> __RLEWriter;
typedef ZigZagStreamWriter<__RLEWriter, int64_t>   __ZigZagWriter;
typedef DeltaStreamWriter<__ZigZagWriter, int64_t> DeltaRLEWriter;

// Base128 -> RLE -> ZigZag -> Delta -> int64_t
typedef RLEStreamReader<int64_t> __RLEReader;
typedef ZigZagStreamReader<__RLEReader, int64_t>   __ZigZagReader;
typedef DeltaStreamReader<__ZigZagReader, int64_t> DeltaRLEReader;
}
