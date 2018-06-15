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
#include "akumuli_version.h"
#include "util.h"

namespace Akumuli {

typedef std::vector<unsigned char> ByteVector;


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

    /** Read base 128 encoded integer from the binary stream
      * FwdIter - forward iterator.
      */
    template<class IOVecBlock>
    u32 get(const IOVecBlock* block, u32 begin) {
        auto acc = TVal();
        auto cnt = TVal();
        auto p = begin;

        while (true) {
            if (p == block->size()) {
                return begin;
            }
            u8 byte = block->get(p);
            auto i = static_cast<u8>(byte & 0x7F);
            acc |= TVal(i) << cnt;
            p++;
            if ((byte & 0x80) == 0) {
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

    /** Write base 128 encoded integer to the binary stream.
      * @returns 'begin' on error, iterator to next free region otherwise
      */
    template<class BlockT>
    bool put(BlockT* block) const {
        TVal value = value_;
        while (true) {
            TVal s = value & 0x7F;
            value >>= 7;
            if (value != 0) {
                s |= 0x80;
            }
            if (!block->safe_put(s)) {
                return false;
            }
            if (value == 0) {
                break;
            }
        }
        return true;
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

template<class BlockT>
struct IOVecVByteStreamWriter {
    BlockT*   block_;
    // tail elements
    u32       cnt_;
    u64       prev_;

    IOVecVByteStreamWriter(BlockT* block)
        : block_(block)
        , cnt_(0)
        , prev_(0)
    {}

    IOVecVByteStreamWriter(IOVecVByteStreamWriter& other)
        : block_(other.block_)
        , cnt_(0)
        , prev_(0)
    {}

    bool empty() const { return cnt_ == 0; }

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
        if (block_->space_left() < (1 + fstctrl + sndctrl)) {
            return false;
        }
        // Write ctrl world
        block_->put(ctrlword);
        for (int i = 0; i < fstctrl; i++) {
            block_->put(static_cast<u8>(fst));
            fst >>= 8;
        }
        for (int i = 0; i < sndctrl; i++) {
            block_->put(static_cast<u8>(snd));
            snd >>= 8;
        }
        return true;
    }

    /** This method is used by DeltaDelta coding.
      */
    template<class TVal> bool put_base128(TVal value) {
        Base128Int<TVal> val(value);
        while (true) {
            if (!val.put(block_)) {
                return false;
            }
            return true;
        }
    }

    bool shortcut() {
        // put sentinel
        if (block_->space_left() == 0) {
            return false;
        }
        block_->put(0xFF);
        return true;
    }

    /** Put value into stream (transactional).
      */
    template <class TVal> bool tput(TVal const* iter, size_t n) {
        assert(n % 2 == 0);  // n expected to be eq 16
        auto oldpos = block_->get_write_pos();
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
                    block_->set_write_pos(oldpos);
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
        if (block_->space_left() < static_cast<i32>(sizeof(TVal))) {
            return false;
        }
        block_->put(value);
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
            if (block_->space_left() < (1 + ctrl)) {
                return false;
            }
            // Write ctrl world
            block_->put(ctrlword);
            for (int i = 0; i < ctrl; i++) {
                block_->put(static_cast<u8>(prev_));
                prev_ >>= 8;
            }
        }
        return true;
    }

    size_t size() const { return block_->size(); }

    size_t space_left() const { return block_->space_left(); }

    /** Try to allocate space inside a stream in current position without
      * compression (needed for size prefixes).
      * @returns pointer to the value inside the stream or nullptr
      */
    template <class T> T* allocate() {
        return block_->template allocate<T>();
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

//! Base128 decoder
template<class BlockT>
struct IOVecStreamReader {
    BlockT* block_;
    u32     pos_;
    u32     cnt_;
    int     ctrl_;
    int     scut_elements_;

    enum {
        CHUNK_SIZE = 16,
    };

    IOVecStreamReader(const BlockT* block)
        : block_(block)
        , pos_(0)
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
            TVal byte = block_->get(pos_);
            acc |= (byte << i);
            pos_++;
        }
        return acc;
    }

    template <class TVal> TVal next_base128() {
        Base128Int<TVal> value;
        auto p = value.get(block_, pos_);
        if (p == pos_) {
            AKU_PANIC("can't read value, out of bounds");
        }
        pos_ = p;
        return static_cast<TVal>(value);
    }

    //! Read uncompressed value from stream
    template <class TVal> TVal read_raw() {
        size_t sz = sizeof(TVal);
        if (block_->size() - pos_ < sz) {
            AKU_PANIC("can't read value, out of bounds");
        }
        auto val = block_->template get_raw<TVal>(pos_);
        pos_ += sz;
        return val;
    }

    size_t space_left() const { return block_->space_left(); }

    const u32 pos() const { return pos_; }
};

template <size_t Step, typename TVal, typename StreamT=VByteStreamWriter>
struct DeltaDeltaStreamWriter {
    StreamT&   stream_;
    TVal                 prev_;
    int                  put_calls_;

    DeltaDeltaStreamWriter(StreamT& stream)
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

static const int PREDICTOR_N = 1 << 7;

template<class StreamT>
static inline bool encode_value(StreamT& wstream, u64 diff, unsigned char flag) {
    int nbytes = (flag & 7) + 1;
    int nshift = (64 - nbytes*8)*(flag >> 3);
    diff >>= nshift;
    switch(nbytes) {
    case 8:
        if (!wstream.put_raw(diff)) {
            return false;
        }
        break;
    case 7:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 6:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 5:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 4:
        if (!wstream.put_raw(static_cast<u32>(diff & 0xFFFFFFFF))) {
            return false;
        }
        diff >>= 32;
        break;
    case 3:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 2:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 1:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
    }
    return true;
}

//! Double to FCM encoder
template<class StreamT=VByteStreamWriter>
struct FcmStreamWriter {
    StreamT&             stream_;
    PredictorT           predictor_;
    u64                  prev_diff_;
    unsigned char        prev_flag_;
    int                  nelements_;

    FcmStreamWriter(StreamT& stream)
        : stream_(stream)
        , predictor_(PREDICTOR_N)
        , prev_diff_(0)
        , prev_flag_(0)
        , nelements_(0)
    {
    }


    bool tput(double const* values, size_t n) {
        assert(n == 16);
        u8  flags[16];
        u64 diffs[16];
        for (u32 i = 0; i < n; i++) {
            std::tie(diffs[i], flags[i]) = encode(values[i]);
        }
        u64 sum_diff = 0;
        for (u32 i = 0; i < n; i++) {
            sum_diff |= diffs[i];
        }
        if (sum_diff == 0) {
            // Shortcut
            if (!stream_.put_raw((u8)0xFF)) {
                return false;
            }
        } else {
            for (size_t i = 0; i < n; i+=2) {
                u64 prev_diff, curr_diff;
                unsigned char prev_flag, curr_flag;
                prev_diff = diffs[i];
                curr_diff = diffs[i+1];
                prev_flag = flags[i];
                curr_flag = flags[i+1];
                if (curr_flag == 0xF) {
                    curr_flag = 0;
                }
                if (prev_flag == 0xF) {
                    prev_flag = 0;
                }
                unsigned char flags = static_cast<unsigned char>((prev_flag << 4) | curr_flag);
                if (!stream_.put_raw(flags)) {
                    return false;
                }
                if (!encode_value(stream_, prev_diff, prev_flag)) {
                    return false;
                }
                if (!encode_value(stream_, curr_diff, curr_flag)) {
                    return false;
                }
            }
        }
        return commit();
    }

    std::tuple<u64, unsigned char> encode(double value) {
        union {
            double real;
            u64 bits;
        } curr = {};
        curr.real = value;
        u64 predicted = predictor_.predict_next();
        predictor_.update(curr.bits);
        u64 diff = curr.bits ^ predicted;

        // Number of trailing and leading zero-bytes
        int leading_bytes = 8;
        int trailing_bytes = 8;

        if (diff != 0) {
            trailing_bytes = __builtin_ctzl(diff) / 8;
            leading_bytes = __builtin_clzl(diff) / 8;
        } else {
            // Fast path for 0-diff values.
            // Flags 7 and 15 are interchangeable.
            // If there is 0 trailing zero bytes and 0 leading bytes
            // code will always generate flag 7 so we can use flag 17
            // for something different (like 0 indication)
            return std::make_tuple(0, 0xF);
        }

        int nbytes;
        unsigned char flag;

        if (trailing_bytes > leading_bytes) {
            // this would be the case with low precision values
            nbytes = 8 - trailing_bytes;
            if (nbytes > 0) {
                nbytes--;
            }
            // 4th bit indicates that only leading bytes are stored
            flag = 8 | (nbytes&7);
        } else {
            nbytes = 8 - leading_bytes;
            if (nbytes > 0) {
                nbytes--;
            }
            // zeroed 4th bit indicates that only trailing bytes are stored
            flag = nbytes&7;
        }
        return std::make_tuple(diff, flag);
    }

    bool put(double value) {
        u64 diff;
        unsigned char flag;
        std::tie(diff, flag) = encode(value);
        if (flag == 0xF) {
            flag = 0;  // Just store one byte, space opt. is disabled
        }
        if (nelements_ % 2 == 0) {
            prev_diff_ = diff;
            prev_flag_ = flag;
        } else {
            // we're storing values by pairs to save space
            unsigned char flags = (prev_flag_ << 4) | flag;
            if (!stream_.put_raw(flags)) {
                return false;
            }
            if (!encode_value(stream_, prev_diff_, prev_flag_)) {
                return false;
            }
            if (!encode_value(stream_, diff, flag)) {
                return false;
            }
        }
        nelements_++;
        return true;
    }

    size_t size() const { return stream_.size(); }

    bool commit() {
        if (nelements_ % 2 != 0) {
            // `input` contains odd number of values so we should use
            // empty second value that will take one byte in output
            unsigned char flags = prev_flag_ << 4;
            if (!stream_.put_raw(flags)) {
                return false;
            }
            if (!encode_value(stream_, prev_diff_, prev_flag_)) {
                return false;
            }
            if (!encode_value(stream_, 0ull, 0)) {
                return false;
            }
        }
        return stream_.commit();
    }
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
    FcmStreamWriter<>   val_stream_;
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


/**
 * Vectorized compressor.
 * This class is intended to be used with vector I/O
 * to save memory (the block can allocate memory in
 * step and write everything at once using vectorized
 * I/O).
 */
template<class BlockT>
struct IOVecBlockWriter {
    enum {
        CHUNK_SIZE  = 16,
        CHUNK_MASK  = 15,
        HEADER_SIZE = 14,  // 2 (version) + 2 (nchunks) + 2 (tail size) + 8 (series id)
    };
    typedef IOVecVByteStreamWriter<BlockT> StreamT;
    typedef DeltaDeltaStreamWriter<16, u64, StreamT> DeltaDeltaWriterT;
    StreamT                  stream_;
    DeltaDeltaWriterT        ts_stream_;
    FcmStreamWriter<StreamT> val_stream_;
    int                      write_index_;
    aku_Timestamp            ts_writebuf_[CHUNK_SIZE];   //! Write buffer for timestamps
    double                   val_writebuf_[CHUNK_SIZE];  //! Write buffer for values
    u16*                     nchunks_;
    u16*                     ntail_;

    //! Empty c-tor. Constructs unwritable object.
    IOVecBlockWriter()
        : stream_(nullptr)
        , ts_stream_(stream_)
        , val_stream_(stream_)
        , write_index_(0)
        , nchunks_(nullptr)
        , ntail_(nullptr)
    {
    }

    /** C-tor
      * @param id Series id.
      * @param size Block size.
      * @param buf Pointer to buffer.
      */
    IOVecBlockWriter(BlockT* block)
        : stream_(block)
        , ts_stream_(stream_)
        , val_stream_(stream_)
        , write_index_(0)
    {
    }

    void init(aku_ParamId id) {
        // offset 0
        auto success = stream_.template put_raw<u16>(AKUMULI_VERSION);
        // offset 2
        nchunks_ = stream_.template allocate<u16>();
        // offset 4
        ntail_ = stream_.template allocate<u16>();
        // offset 6
        success = stream_.put_raw(id) && success;
        if (!success || nchunks_ == nullptr || ntail_ == nullptr) {
            AKU_PANIC("Buffer is too small (3)");
        }
        *ntail_ = 0;
        *nchunks_ = 0;
    }

    /** Append value to block.
      * @param ts Timestamp.
      * @param value Value.
      * @return AKU_EOVERFLOW when block is full or AKU_SUCCESS.
      */
    aku_Status put(aku_Timestamp ts, double value) {
        if (room_for_chunk()) {
            // Invariant 1: number of elements stored in write buffer (ts_writebuf_ val_writebuf_)
            // equals `write_index_ % CHUNK_SIZE`.
            ts_writebuf_[write_index_ & CHUNK_MASK] = ts;
            val_writebuf_[write_index_ & CHUNK_MASK] = value;
            write_index_++;
            if ((write_index_ & CHUNK_MASK) == 0) {
                // put timestamps
                if (ts_stream_.tput(ts_writebuf_, CHUNK_SIZE)) {
                    if (val_stream_.tput(val_writebuf_, CHUNK_SIZE)) {
                        *nchunks_ += 1;
                        return AKU_SUCCESS;
                    }
                }
                // Content of the write buffer was lost, this can happen only if `room_for_chunk`
                // function estimates required space incorrectly.
                assert(false);
                return AKU_EOVERFLOW;
            }
        } else {
            // Put values to the end of the stream without compression.
            // This can happen first only when write buffer is empty.
            assert((write_index_ & CHUNK_MASK) == 0);
            if (stream_.put_raw(ts)) {
                if (stream_.put_raw(value)) {
                    *ntail_ += 1;
                    return AKU_SUCCESS;
                }
            }
            return AKU_EOVERFLOW;
        }
        return AKU_SUCCESS;
    }

    size_t commit() {
        // It should be possible to store up to one million chunks in one block,
        // for 4K block size this is more then enough.
        auto nchunks = write_index_ / CHUNK_SIZE;
        auto buftail = write_index_ % CHUNK_SIZE;
        // Invariant 2: if DataBlockWriter was closed after `put` method overflowed (return AKU_EOVERFLOW),
        // then `ntail_` should be GE then zero and write buffer should be empty (write_index_ = multiple of CHUNK_SIZE).
        // Otherwise, `ntail_` should be zero.
        if (buftail) {
            // Write buffer is not empty
            if (*ntail_ != 0) {
                // invariant is broken
                AKU_PANIC("Write buffer is not empty but can't be flushed");
            }
            for (int ix = 0; ix < buftail; ix++) {
                auto success = stream_.put_raw(ts_writebuf_[ix]);
                success = stream_.put_raw(val_writebuf_[ix]) && success;
                if (!success) {
                    // Data loss. This should never happen at this point. If this error
                    // occures then `room_for_chunk` estimates space requirements incorrectly.
                    assert(false);
                    break;
                }
                *ntail_ += 1;
                write_index_--;
            }
        }
        assert(nchunks <= 0xFFFF);
        *nchunks_ = static_cast<u16>(nchunks);
        return stream_.size();
    }

    //! Read tail elements (the ones not yet written to output stream)
    void read_tail_elements(std::vector<aku_Timestamp>* timestamps,
                            std::vector<double>*        values) const {
        // Note: this method can be used to read values from
        // write buffer. It sort of breaks incapsulation but
        // we don't need  to maintain  another  write buffer
        // anywhere else.
        auto tailsize = write_index_ & CHUNK_MASK;
        for (int i = 0; i < tailsize; i++) {
            timestamps->push_back(ts_writebuf_[i]);
            values->push_back(val_writebuf_[i]);
        }
    }

    int get_write_index() const {
        // Note: we need to be able to read this index to
        // get rid of write index inside NBTreeLeaf.
        if (!stream_.empty()) {
            return *ntail_ + write_index_;
        }
        return 0;
    }

private:
    //! Return true if there is enough free space to store `CHUNK_SIZE` compressed values
    bool room_for_chunk() const {
        static const size_t MARGIN = 10*16 + 9*16;  // worst case
        auto free_space = stream_.space_left();
        if (free_space < MARGIN) {
            return false;
        }
        return true;
    }
};

}  // namespace V2
}
