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
#include <tuple>

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

    bool empty() const { return begin_ == end_; }

    //! Put value into stream (transactional).
    template <class TVal> bool tput(TVal const* iter, size_t n) {
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

    //! Put value into stream.
    template <class TVal> bool put(TVal value) {
        Base128Int<TVal> val(value);
        unsigned char*   p = val.put(pos_, end_);
        if (pos_ == p) {
            return false;
        }
        pos_ = p;
        return true;
    }

    template <class TVal> bool put_raw(TVal value) {
        if ((end_ - pos_) < (int)sizeof(TVal)) {
            return false;
        }
        *reinterpret_cast<TVal*>(pos_) = value;
        pos_ += sizeof(value);
        return true;
    }

    //! Commit stream
    bool commit() { return true; }

    size_t size() const { return static_cast<size_t>(pos_ - begin_); }

    size_t space_left() const { return static_cast<size_t>(end_ - pos_); }

    // Try to allocate space inside a stream in current position without
    // compression (needed for size prefixes).
    // @returns pointer to the value inside the stream or nullptr
    template <class T> T* allocate() {
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
        , end_(end) {}

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


/** VByte for DeltaDelta encoding.
  * Delta-RLE encoding used to work great for majority of time-series. But sometimes
  * it doesn't work because timestamps have some noise in low registers. E.g. timestamps
  * have 1s period but each timestamp has nonzero amount of usec in it. In this case 
  * Delta encoding will produce series of different values (probably about 4 bytes each).
  * Run length encoding will have trouble compressing it. Actually it will make output larger
  * then simple delta-encoding (but it will be smaller then input anyway). To solve this
  * DeltaDelta encoding was introduced. After delta encoding step we're searching for smallest
  * value and substract it from each element of the chunk (we're doing it for each chunk).
  * This will make timestamps smaller (1-2 bytes instead of 3-4). But if series of timestamps
  * is regullar Delta-RLE will achive much better results. In this case DeltaDelta will
  * produce one value followed by series of zeroes [42, 0, 0, ... 0].
  * 
  * This encoding was introduced to solve this problem. It combines values into pairs (x1, x2)
  * and writes them using one control byte. This is basically the same as LEB128 but with
  * all control bits moved to separate location. In this case each byte stores control byte or 8-bits
  * of value (7-bits in LEB128). This makes encoder simplier because we can get rid of most branches.
  * Control world consist of two flags (first one corresponds to x1, the second one to x2). Each flag
  * is a size of the value in bytes (size(x1) | (size(x3) << 4)). E.g. if both values can be stored
  * using one byte cotrol word will be 0x11, and if first value can be stored using only one byte
  * and second needs eight bytes control word will be 0x81.
  *
  * It also provides method to store leb128 encoded values to store min values for the DeltaDelta
  * encoder. When 16 values are encoded using DeltaDelta encoder it produce 17 values, min value and
  * 16 delta values. This first min value should be stored using leb128.
  * 
  * To store effectively combination of signle value followed by the series of zeroes special shortcut
  * was introduced. In this case control word will be equall to 0xFF. If decoder encounters 0xFF control
  * world it returns 16 zeroes.
  * This encoding combines upsides of both Delta-RLE and DeltaDelta encodings without its downsides.
  */
struct VByteStreamWriter {
    // underlying memory region
    const u8* begin_;
    const u8* end_;
    u8*       pos_;
    // tail elements
    u32       cnt_;
    u64       prev_;

    VByteStreamWriter(u8* begin, const unsigned char* end)
        : begin_(begin)
        , end_(end)
        , pos_(begin)
        , cnt_(0)
        , prev_(0)
    {}

    VByteStreamWriter(VByteStreamWriter& other)
        : begin_(other.begin_)
        , end_(other.end_)
        , pos_(other.pos_)
        , cnt_(0)
        , prev_(0)
    {}

    bool empty() const { return begin_ == end_; }

    //! Perform combined write (TVal should be integer)
    template<class TVal> bool encode(TVal fst, TVal snd) {
        static_assert(sizeof(TVal) <= 8, "Value is to large");
        int fstlen = 8*sizeof(TVal);
        int sndlen = 8*sizeof(TVal);
        if (fst) {
            fstlen = __builtin_clzl(fst);
        }
        if (snd) {
            sndlen = __builtin_clzl(snd);
        }
        int fstctrl = sizeof(TVal) - fstlen / 8;  // value should be in 0-8 range
        int sndctrl = sizeof(TVal) - sndlen / 8;  // value should be in 0-8 range
        u8 ctrlword = static_cast<u8>(fstctrl | (sndctrl << 4));
        // Check size
        if (space_left() < static_cast<size_t>(1 + fstctrl + sndctrl)) {
            return false;
        }
        // Write ctrl world
        *pos_++ = ctrlword;
        for (int i = 0; i < fstctrl; i++) {
            *pos_++ = static_cast<u8>(fst);
            fst >>= 8;
        }
        for (int i = 0; i < sndctrl; i++) {
            *pos_++ = static_cast<u8>(snd);
            snd >>= 8;
        }
        return true;
    }

    /** This method is used by DeltaDelta coding.
      */
    template<class TVal> bool put_base128(TVal value) {
        Base128Int<TVal> val(value);
        unsigned char*   p = val.put(pos_, end_);
        if (pos_ == p) {
            return false;
        }
        pos_ = p;
        return true;
    }
    
    bool shortcut() {
        // put sentinel
        if (space_left() == 0) {
            return false;
        }
        *pos_++ = 0xFF;
        return true;
    }

    /** Put value into stream (transactional).
      */
    template <class TVal> bool tput(TVal const* iter, size_t n) {
        assert(n % 2 == 0);  // n expected to be eq 16 
        auto oldpos = pos_;
        // Fast path for DeltaDelta encoding
        bool take_shortcut = true;
        for (u32 i = 0; i < n; i++) {
            if (iter[i] != 0) {
                take_shortcut = false;
                break;
            }
        }
        if (take_shortcut) {
            return shortcut();
        } else {
            for (u32 i = 0; i < n; i+=2) {
                if (!encode(iter[i], iter[i+1])) {
                    // restore old pos_ value
                    pos_ = oldpos;
                    return false;
                }
            }
        }
        return true;
    }

    /** Put value into stream. This method should be used after
      * tput. The idea is that user should write most of the data
      * using `tput` method and then the rest of the data (less then 
      * chunksize elements) should be written using this one. If one
      * call `put` before `tput` stream will be broken and unreadable.
      */
    template <class TVal> bool put(TVal value) {
        cnt_++;
        union {
            TVal val;
            u64 uint;
        } prev;
        if (cnt_ % 2 != 0) {
            prev.val = value;
            prev_ = prev.uint;
        } else {
            prev.uint = prev_;
            return encode(prev.val, value);
        }
        return true;
    }

    template <class TVal> bool put_raw(TVal value) {
        if ((end_ - pos_) < static_cast<i32>(sizeof(TVal))) {
            return false;
        }
        *reinterpret_cast<TVal*>(pos_) = value;
        pos_ += sizeof(value);
        return true;
    }

    //! Commit stream
    bool commit() {
        // write tail if needed
        if (cnt_ % 2 == 1) {
            int len = 64;
            if (prev_) {
                len = __builtin_clzl(prev_);
            }
            int ctrl = 8 - len / 8;  // value should be in 0-8 range
            u8 ctrlword = static_cast<u8>(ctrl);
            // Check size
            if (space_left() < static_cast<size_t>(1 + ctrl)) {
                return false;
            }
            // Write ctrl world
            *pos_++ = ctrlword;
            for (int i = 0; i < ctrl; i++) {
                *pos_++ = static_cast<u8>(prev_);
                prev_ >>= 8;
            }
        }
        return true;
    }

    size_t size() const { return static_cast<size_t>(pos_ - begin_); }

    size_t space_left() const { return static_cast<size_t>(end_ - pos_); }

    /** Try to allocate space inside a stream in current position without
      * compression (needed for size prefixes).
      * @returns pointer to the value inside the stream or nullptr
      */
    template <class T> T* allocate() {
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
struct VByteStreamReader {
    const u8* pos_;
    const u8* end_;
    u32 cnt_;
    int ctrl_;
    int scut_elements_;

    enum {
        CHUNK_SIZE = 16,
    };

    VByteStreamReader(const u8* begin, const u8* end)
        : pos_(begin)
        , end_(end)
        , cnt_(0)
        , ctrl_(0)
        , scut_elements_(0)
    {}

    template <class TVal>
    TVal next() {
        if (ctrl_ == 0xFF && scut_elements_) {
            scut_elements_--;
            cnt_++;
            return 0;
        }
        int bytelen = 0;
        if (cnt_++ % 2 == 0) {
            // Read control byte
            ctrl_ = read_raw<u8>();
            bytelen = ctrl_ & 0xF;
            if ((ctrl_ >> 4) == 0xF) {
                scut_elements_ = CHUNK_SIZE-1;
                return 0;
            }
        } else {
            bytelen = ctrl_ >> 4;
        }
        TVal acc = {};
        if (space_left() < static_cast<size_t>(bytelen)) {
            AKU_PANIC("can't read value, out of bounds");
        }
        for(int i = 0; i < bytelen*8; i += 8) {
            TVal byte = *pos_;
            acc |= (byte << i);
            pos_++;
        }
        return acc;
    }

    template <class TVal> TVal next_base128() {
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

    size_t space_left() const { return static_cast<size_t>(end_ - pos_); }

    const u8* pos() const { return pos_; }
};

template <class Stream, class TVal> struct ZigZagStreamWriter {
    Stream stream_;

    ZigZagStreamWriter(Base128StreamWriter& stream)
        : stream_(stream) {}

    bool tput(TVal const* iter, size_t n) {
        assert(n < 1000);  // 1000 is too high for most uses but it woun't cause stack overflow
        TVal outbuf[n];
        memset(outbuf, 0, n);
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

template <class Stream, typename TVal> struct DeltaStreamWriter {
    Stream stream_;
    TVal   prev_;

    template<class Substream>
    DeltaStreamWriter(Substream& stream)
        : stream_(stream)
        , prev_{}
    {}

    bool tput(TVal const* iter, size_t n) {
        assert(n < 1000);
        TVal outbuf[n];
        memset(outbuf, 0, n);
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
        prev_       = value;
        return result;
    }

    size_t size() const { return stream_.size(); }

    bool commit() { return stream_.commit(); }
};


template <class Stream, typename TVal> struct DeltaStreamReader {
    Stream stream_;
    TVal   prev_;

    template<class Substream>
    DeltaStreamReader(Substream& stream)
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


template <size_t Step, typename TVal> struct DeltaDeltaStreamWriter {
    VByteStreamWriter&   stream_;
    TVal                 prev_;
    int                  put_calls_;

    DeltaDeltaStreamWriter(VByteStreamWriter& stream)
        : stream_(stream)
        , prev_()
        , put_calls_(0) {}

    bool tput(TVal const* iter, size_t n) {
        assert(n == Step);
        TVal outbuf[n];
        TVal min = iter[0] - prev_;
        for (size_t i = 0; i < n; i++) {
            auto value  = iter[i];
            auto result = value - prev_;
            outbuf[i]   = result;
            prev_       = value;
            min         = std::min(result, min);
        }
        if (!stream_.put_base128(min)) {
            return false;
        }
        for (size_t i = 0; i < n; i++) {
            outbuf[i] -= min;
        }
        return stream_.tput(outbuf, n);
    }

    bool put(TVal value) {
        bool success = false;
        if (put_calls_ == 0) {
            // put fake min value
            success = stream_.put_base128(0);
            if (!success) {
                return false;
            }
        }
        put_calls_++;
        success = stream_.put(value - prev_);
        prev_   = value;
        return success;
    }

    size_t size() const { return stream_.size(); }

    bool commit() { return stream_.commit(); }
};

template <size_t Step, typename TVal> struct DeltaDeltaStreamReader {
    VByteStreamReader&   stream_;
    TVal                 prev_;
    TVal                 min_;
    int                  counter_;

    DeltaDeltaStreamReader(VByteStreamReader& stream)
        : stream_(stream)
        , prev_()
        , min_()
        , counter_() {}

    TVal next() {
        if (counter_ % Step == 0) {
            // read min
            min_ = stream_.next_base128<TVal>();
        }
        counter_++;
        TVal delta = stream_.next<TVal>();
        TVal value = prev_ + delta + min_;
        prev_      = value;
        return value;
    }

    const unsigned char* pos() const { return stream_.pos(); }
};

template <typename TVal> struct RLEStreamWriter {
    Base128StreamWriter& stream_;
    TVal                 prev_;
    TVal                 reps_;
    size_t               start_size_;

    RLEStreamWriter(Base128StreamWriter& stream)
        : stream_(stream)
        , prev_()
        , reps_()
        , start_size_(stream.size()) {}

    bool tput(TVal const* iter, size_t n) {
        size_t outpos = 0;
        TVal   outbuf[n * 2];
        memset(outbuf, 0, n);
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
        if (outpos < n * 2) {
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

    bool commit() { return stream_.put(reps_) && stream_.put(prev_) && stream_.commit(); }
};

template <typename TVal> struct RLEStreamReader {
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

struct SimplePredictor {
    u64 last_value;

    SimplePredictor(size_t);

    u64 predict_next() const;

    void update(u64 value);
};

struct FcmPredictor {
    std::vector<u64> table;
    u64              last_hash;
    const u64        MASK_;

    FcmPredictor(size_t table_size);

    u64 predict_next() const;

    void update(u64 value);
};

struct DfcmPredictor {
    std::vector<u64> table;
    u64              last_hash;
    u64              last_value;
    const u64        MASK_;

    //! C-tor. `table_size` should be a power of two.
    DfcmPredictor(int table_size);

    u64 predict_next() const;

    void update(u64 value);
};

// 2nd order DFCM predictor
struct Dfcm2Predictor {
    std::vector<u64> table1;
    std::vector<u64> table2;
    u64              last_hash;
    u64              last_value1;
    u64              last_value2;
    const u64        MASK_;

    //! C-tor. `table_size` should be a power of two.
    Dfcm2Predictor(int table_size);

    u64 predict_next() const;

    void update(u64 value);
};

typedef DfcmPredictor PredictorT;
//typedef SimplePredictor PredictorT;

//! Double to FCM encoder
struct FcmStreamWriter {
    VByteStreamWriter&   stream_;
    PredictorT           predictor_;
    u64                  prev_diff_;
    unsigned char        prev_flag_;
    int                  nelements_;

    FcmStreamWriter(VByteStreamWriter& stream);

    inline std::tuple<u64, unsigned char> encode(double value);

    bool tput(double const* values, size_t n);

    bool put(double value);

    size_t size() const;

    bool commit();
};

//! FCM to double decoder
struct FcmStreamReader {
    VByteStreamReader&   stream_;
    PredictorT           predictor_;
    u32                  flags_;
    u32                  iter_;
    u32                  nzeroes_;

    FcmStreamReader(VByteStreamReader& stream);

    double next();

    const u8* pos() const;
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

// Old depricated functions
struct CompressionUtil {

    /** Compress and write ChunkHeader to memory stream.
      * @param n_elements out parameter - number of written elements
      * @param ts_begin out parameter - first timestamp
      * @param ts_end out parameter - last timestamp
      * @param data ChunkHeader to compress
      */
    static aku_Status encode_chunk(u32* n_elements, aku_Timestamp* ts_begin, aku_Timestamp* ts_end,
                                   ChunkWriter* writer, const UncompressedChunk& data);

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
                                   const unsigned char* pend, u32 nelements);

    /** Compress list of doubles.
      * @param input array of doubles
      * @param params array of parameter ids
      * @param buffer resulting byte array
      */
    static size_t compress_doubles(const std::vector<double>& input, Base128StreamWriter &wstream);

    /** Decompress list of doubles.
      * @param buffer input data
      * @param numbloks number of 4bit blocs inside buffer
      * @param params list of parameter ids
      * @param output resulting array
      */
    static void decompress_doubles(Base128StreamReader &rstream, size_t numvalues,
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
typedef RLEStreamWriter<u32> RLELenWriter;
// Base128 -> RLE -> Length
typedef RLEStreamReader<u32> RLELenReader;

// i64 -> Delta -> ZigZag -> RLE -> Base128
typedef RLEStreamWriter<i64> __RLEWriter;
typedef ZigZagStreamWriter<__RLEWriter, i64>   __ZigZagWriter;
typedef DeltaStreamWriter<__ZigZagWriter, i64> ZDeltaRLEWriter;

// Base128 -> RLE -> ZigZag -> Delta -> i64
typedef RLEStreamReader<i64> __RLEReader;
typedef ZigZagStreamReader<__RLEReader, i64>   __ZigZagReader;
typedef DeltaStreamReader<__ZigZagReader, i64> ZDeltaRLEReader;

typedef DeltaStreamWriter<RLEStreamWriter<u64>, u64> DeltaRLEWriter;
typedef DeltaStreamReader<RLEStreamReader<u64>, u64> DeltaRLEReader;


typedef DeltaDeltaStreamReader<16, u64> DeltaDeltaReader;
typedef DeltaDeltaStreamWriter<16, u64> DeltaDeltaWriter;


namespace StorageEngine {

struct DataBlockWriter {
    enum {
        CHUNK_SIZE  = 16,
        CHUNK_MASK  = 15,
        HEADER_SIZE = 14,  // 2 (version) + 2 (nchunks) + 2 (tail size) + 8 (series id)
    };
    VByteStreamWriter   stream_;
    DeltaDeltaWriter    ts_stream_;
    FcmStreamWriter     val_stream_;
    int                 write_index_;
    aku_Timestamp       ts_writebuf_[CHUNK_SIZE];   //! Write buffer for timestamps
    double              val_writebuf_[CHUNK_SIZE];  //! Write buffer for values
    u16*                nchunks_;
    u16*                ntail_;

    //! Empty c-tor. Constructs unwritable object.
    DataBlockWriter();

    /** C-tor
      * @param id Series id.
      * @param size Block size.
      * @param buf Pointer to buffer.
      */
    DataBlockWriter(aku_ParamId id, u8* buf, int size);

    /** Append value to block.
      * @param ts Timestamp.
      * @param value Value.
      * @return AKU_EOVERFLOW when block is full or AKU_SUCCESS.
      */
    aku_Status put(aku_Timestamp ts, double value);

    size_t commit();

    //! Read tail elements (the ones not yet written to output stream)
    void read_tail_elements(std::vector<aku_Timestamp>* timestamps,
                            std::vector<double>*        values) const;

    int get_write_index() const;

private:
    //! Return true if there is enough free space to store `CHUNK_SIZE` compressed values
    bool room_for_chunk() const;
};

struct DataBlockReader {
    enum {
        CHUNK_SIZE = 16,
        CHUNK_MASK = 15,
    };
    const u8*           begin_;
    VByteStreamReader   stream_;
    DeltaDeltaReader    ts_stream_;
    FcmStreamReader     val_stream_;
    aku_Timestamp       read_buffer_[CHUNK_SIZE];
    u32                 read_index_;

    DataBlockReader(u8 const* buf, size_t bufsize);

    std::tuple<aku_Status, aku_Timestamp, double> next();

    size_t nelements() const;

    aku_ParamId get_id() const;

    u16 version() const;
};

}  // namespace V2
}
