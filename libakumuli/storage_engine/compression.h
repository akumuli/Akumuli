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
#include <array>

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
            if (p == static_cast<u32>(block->size())) {
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

    /**
     * @brief Don't interpret next 'n' bytes
     * @param n is a number of bytes to skip
     * @return pointer to the begining of the skiped region
     */
    u8* skip(u32 n) {
        return block_->allocate(n);
    }

    bool empty() const { return block_->size() == 0; }

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
        block_->put(static_cast<u8>(0xFF));
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
        block_->template put<TVal>(value);
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
struct IOVecVByteStreamReader {
    const BlockT* block_;
    u32           pos_;
    u32           cnt_;
    int           ctrl_;
    int           scut_elements_;

    enum {
        CHUNK_SIZE = 16,
    };

    IOVecVByteStreamReader(const BlockT* block)
        : block_(block)
        , pos_(0)
        , cnt_(0)
        , ctrl_(0)
        , scut_elements_(0)
    {}

    /**
     * @brief Don't interpret next 'n' bytes
     * @param n is a number of bytes to skip
     * @return pointer to the begining of the skiped region
     */
    const u8* skip(u32 n) {
        const u8* data = block_->get_cdata(0) + pos_;
        pos_ += n;
        return data;
    }

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

    size_t space_left() const { return block_->bytes_to_read(pos_); }

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

template <size_t Step, typename TVal, typename StreamT=VByteStreamReader>
struct DeltaDeltaStreamReader {
    StreamT&             stream_;
    TVal                 prev_;
    TVal                 min_;
    int                  counter_;

    DeltaDeltaStreamReader(StreamT& stream)
        : stream_(stream)
        , prev_()
        , min_()
        , counter_() {}

    TVal next() {
        if (counter_ % Step == 0) {
            // read min
            min_ = stream_.template next_base128<TVal>();
        }
        counter_++;
        TVal delta = stream_.template next<TVal>();
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

static const int PREDICTOR_N = 1 << 7;

struct DfcmPredictor {
    std::array<u64, PREDICTOR_N> table;
    u64                          last_hash;
    u64                          last_value;
    const u64                    MASK_;

    //! C-tor. `table_size` should be a power of two.
    DfcmPredictor(int table_size);

    u64 predict_next() const;

    void update(u64 value);
};

typedef DfcmPredictor PredictorT;

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

    template<typename T>
    bool _packN(const u64* input) {
        for (int i = 0; i < 16; i++) {
            T bits = static_cast<T>(input[i]);
            if (!stream_.put_raw(bits)) {
                return false;
            }
        }
        return true;
    }

    bool _pack1(const u64* input) {
        u16 bits = 0;
        for (int i = 0; i < 16; i++) {
            bits |= static_cast<u16>((input[i] & 1) << i);
        }
        if (!stream_.put_raw(bits)) {
            return false;
        }
        return true;
    }

    bool _pack2(const u64* input) {
        u32 bits = 0;
        for (int i = 0; i < 16; i++) {
            bits |= static_cast<u32>((input[i] & 3) << 2*i);
        }
        if (!stream_.put_raw(bits)) {
            return false;
        }
        return true;
    }

    bool _pack3(const u64* input) {
        u32 bits0 = 0;
        u16 bits1 = 0;
        bits0 |= static_cast<u32>((input[0]  & 7));
        bits0 |= static_cast<u32>((input[1]  & 7) << 3);
        bits0 |= static_cast<u32>((input[2]  & 7) << 6);
        bits0 |= static_cast<u32>((input[3]  & 7) << 9);
        bits0 |= static_cast<u32>((input[4]  & 7) << 12);
        bits0 |= static_cast<u32>((input[5]  & 7) << 15);
        bits0 |= static_cast<u32>((input[6]  & 7) << 18);
        bits0 |= static_cast<u32>((input[7]  & 7) << 21);
        bits0 |= static_cast<u32>((input[8]  & 7) << 24);
        bits0 |= static_cast<u32>((input[9]  & 7) << 27);
        bits0 |= static_cast<u32>((input[10] & 3) << 30);
        bits1 |= static_cast<u32>((input[10] & 4) >> 2);
        bits1 |= static_cast<u32>((input[11] & 7) << 1);
        bits1 |= static_cast<u32>((input[12] & 7) << 4);
        bits1 |= static_cast<u32>((input[13] & 7) << 7);
        bits1 |= static_cast<u32>((input[14] & 7) << 10);
        bits1 |= static_cast<u32>((input[15] & 7) << 13);
        if (!stream_.put_raw(bits0)) {
            return false;
        }
        if (!stream_.put_raw(bits1)) {
            return false;
        }
        return true;
    }

    bool _pack4(const u64* input) {
        u64 bits0 = 0;
        bits0 |= static_cast<u64>((input[0]  & 0xF));
        bits0 |= static_cast<u64>((input[1]  & 0xF) << 4);
        bits0 |= static_cast<u64>((input[2]  & 0xF) << 8);
        bits0 |= static_cast<u64>((input[3]  & 0xF) << 12);
        bits0 |= static_cast<u64>((input[4]  & 0xF) << 16);
        bits0 |= static_cast<u64>((input[5]  & 0xF) << 20);
        bits0 |= static_cast<u64>((input[6]  & 0xF) << 24);
        bits0 |= static_cast<u64>((input[7]  & 0xF) << 28);
        bits0 |= static_cast<u64>((input[8]  & 0xF) << 32);
        bits0 |= static_cast<u64>((input[9]  & 0xF) << 36);
        bits0 |= static_cast<u64>((input[10] & 0xF) << 40);
        bits0 |= static_cast<u64>((input[11] & 0xF) << 44);
        bits0 |= static_cast<u64>((input[12] & 0xF) << 48);
        bits0 |= static_cast<u64>((input[13] & 0xF) << 52);
        bits0 |= static_cast<u64>((input[14] & 0xF) << 56);
        bits0 |= static_cast<u64>((input[15] & 0xF) << 60);
        if (!stream_.put_raw(bits0)) {
            return false;
        }
        return true;
    }

    bool _pack5(const u64* input) {
        u64 bits0 = 0;
        u16 bits1 = 0;
        bits0 |= static_cast<u64>((input[0]  & 0x1F));
        bits0 |= static_cast<u64>((input[1]  & 0x1F) << 5);
        bits0 |= static_cast<u64>((input[2]  & 0x1F) << 10);
        bits0 |= static_cast<u64>((input[3]  & 0x1F) << 15);
        bits0 |= static_cast<u64>((input[4]  & 0x1F) << 20);
        bits0 |= static_cast<u64>((input[5]  & 0x1F) << 25);
        bits0 |= static_cast<u64>((input[6]  & 0x1F) << 30);
        bits0 |= static_cast<u64>((input[7]  & 0x1F) << 35);
        bits0 |= static_cast<u64>((input[8]  & 0x1F) << 40);
        bits0 |= static_cast<u64>((input[9]  & 0x1F) << 45);
        bits0 |= static_cast<u64>((input[10] & 0x1F) << 50);
        bits0 |= static_cast<u64>((input[11] & 0x1F) << 55);
        bits0 |= static_cast<u64>((input[12] & 0x0F) << 60);
        bits1 |= static_cast<u32>((input[12] & 0x10) >> 4);
        bits1 |= static_cast<u32>((input[13] & 0x1F) << 1);
        bits1 |= static_cast<u32>((input[14] & 0x1F) << 6);
        bits1 |= static_cast<u32>((input[15] & 0x1F) << 11);
        if (!stream_.put_raw(bits0)) {
            return false;
        }
        if (!stream_.put_raw(bits1)) {
            return false;
        }
        return true;
    }

    bool _pack6(const u64* input) {
        u64 bits0 = 0;
        u32 bits1 = 0;
        bits0 |= static_cast<u64>((input[0]  & 0x3F));
        bits0 |= static_cast<u64>((input[1]  & 0x3F) << 6);
        bits0 |= static_cast<u64>((input[2]  & 0x3F) << 12);
        bits0 |= static_cast<u64>((input[3]  & 0x3F) << 18);
        bits0 |= static_cast<u64>((input[4]  & 0x3F) << 24);
        bits0 |= static_cast<u64>((input[5]  & 0x3F) << 30);
        bits0 |= static_cast<u64>((input[6]  & 0x3F) << 36);
        bits0 |= static_cast<u64>((input[7]  & 0x3F) << 42);
        bits0 |= static_cast<u64>((input[8]  & 0x3F) << 48);
        bits0 |= static_cast<u64>((input[9]  & 0x3F) << 54);
        bits0 |= static_cast<u64>((input[10] & 0x0F) << 60);
        bits1 |= static_cast<u32>((input[10] & 0x30) >> 4);
        bits1 |= static_cast<u32>((input[11] & 0x3F) << 2);
        bits1 |= static_cast<u32>((input[12] & 0x3F) << 8);
        bits1 |= static_cast<u32>((input[13] & 0x3F) << 14);
        bits1 |= static_cast<u32>((input[14] & 0x3F) << 20);
        bits1 |= static_cast<u32>((input[15] & 0x3F) << 26);
        if (!stream_.put_raw(bits0)) {
            return false;
        }
        if (!stream_.put_raw(bits1)) {
            return false;
        }
        return true;
    }

    bool _pack7(const u64* input) {
        u64 bits0 = 0;
        u32 bits1 = 0;
        u16 bits2 = 0;
        bits0 |= static_cast<u64>((input[0]  & 0x7F));
        bits0 |= static_cast<u64>((input[1]  & 0x7F) << 7);
        bits0 |= static_cast<u64>((input[2]  & 0x7F) << 14);
        bits0 |= static_cast<u64>((input[3]  & 0x7F) << 21);
        bits0 |= static_cast<u64>((input[4]  & 0x7F) << 28);
        bits0 |= static_cast<u64>((input[5]  & 0x7F) << 35);
        bits0 |= static_cast<u64>((input[6]  & 0x7F) << 42);
        bits0 |= static_cast<u64>((input[7]  & 0x7F) << 49);
        bits0 |= static_cast<u64>((input[8]  & 0x7F) << 56);
        bits0 |= static_cast<u64>((input[9]  & 0x01) << 63);
        bits1 |= static_cast<u32>((input[9]  & 0x7E) >> 1);
        bits1 |= static_cast<u32>((input[10] & 0x7F) << 6);
        bits1 |= static_cast<u32>((input[11] & 0x7F) << 13);
        bits1 |= static_cast<u32>((input[12] & 0x7F) << 20);
        bits1 |= static_cast<u32>((input[13] & 0x1F) << 27);
        bits2 |= static_cast<u16>((input[13] & 0x60) >> 5);
        bits2 |= static_cast<u16>((input[14] & 0x7F) << 2);
        bits2 |= static_cast<u16>((input[15] & 0x7F) << 9);
        if (!stream_.put_raw(bits0)) {
            return false;
        }
        if (!stream_.put_raw(bits1)) {
            return false;
        }
        if (!stream_.put_raw(bits2)) {
            return false;
        }
        return true;
    }

    template<typename T>
    void _shiftN(u64* input) {
        for (int i = 0; i < 16; i++) {
            input[i] >>= 8*sizeof(T);
        }
    }

    bool pack(u64* input, int n) {
        switch(n) {
        case 0:
            return true;
        case 1:
            return _pack1(input);
        case 2:
            return _pack2(input);
        case 3:
            return _pack3(input);
        case 4:
            return _pack4(input);
        case 5:
            return _pack5(input);
        case 6:
            return _pack6(input);
        case 7:
            return _pack7(input);
        case 8:
            return _packN<u8>(input);
        case 9:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack1(input);
        case 10:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack2(input);
        case 11:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack3(input);
        case 12:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack4(input);
        case 13:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack5(input);
        case 14:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack6(input);
        case 15:
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack7(input);
        case 16:
            return _packN<u16>(input);
        case 17:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack1(input);
        case 18:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack2(input);
        case 19:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack3(input);
        case 20:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack4(input);
        case 21:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack5(input);
        case 22:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack6(input);
        case 23:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack7(input);
        case 24:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _packN<u8>(input);
        case 25:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack1(input);
        case 26:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack2(input);
        case 27:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack3(input);
        case 28:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack4(input);
        case 29:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack5(input);
        case 30:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack6(input);
        case 31:
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack7(input);
        case 32:
            return _packN<u32>(input);
        case 33:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack1(input);
        case 34:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack2(input);
        case 35:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack3(input);
        case 36:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack4(input);
        case 37:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack5(input);
        case 38:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack6(input);
        case 39:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _pack7(input);
        case 40:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _packN<u8>(input);
        case 41:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack1(input);
        case 42:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack2(input);
        case 43:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack3(input);
        case 44:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack4(input);
        case 45:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack5(input);
        case 46:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack6(input);
        case 47:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack7(input);
        case 48:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            return _packN<u16>(input);
        case 49:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack1(input);
        case 50:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack2(input);
        case 51:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack3(input);
        case 52:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack4(input);
        case 53:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack5(input);
        case 54:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack6(input);
        case 55:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _pack7(input);
        case 56:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            return _packN<u8>(input);
        case 57:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack1(input);
        case 58:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack2(input);
        case 59:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack3(input);
        case 60:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack4(input);
        case 61:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack5(input);
        case 62:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack6(input);
        case 63:
            if (!_packN<u32>(input)) {
                return false;
            }
            _shiftN<u32>(input);
            if (!_packN<u16>(input)) {
                return false;
            }
            _shiftN<u16>(input);
            if (!_packN<u8>(input)) {
                return false;
            }
            _shiftN<u8>(input);
            return _pack7(input);
        case 64:
            return _packN<u64>(input);
        }
        return false;
    }

    bool tput(double const* values, size_t n) {
        assert(n == 16);
        u8  flags[16];
        u64 diffs[16];
        int trailing_bits_min = 64;
        int leading_bits_min  = 64;
        int size_estimate = 8;
        for (u32 i = 0; i < n; i++) {
            int leading_bits, trailing_bits;
            std::tie(diffs[i], flags[i], leading_bits, trailing_bits) = encode(values[i]);
            trailing_bits_min = std::min(trailing_bits_min, trailing_bits);
            leading_bits_min  = std::min(leading_bits_min,  leading_bits);
            size_estimate += (flags[i] & 0x7) + 1;
        }
        int shortcut = 0;
        if (trailing_bits_min > leading_bits_min) {
            int bytes_total = ((64 - trailing_bits_min) * 16 + 7) / 8 + 2;
            if (bytes_total < size_estimate) {
                shortcut = 1;
            }
        } else {
            int bytes_total = ((64 - leading_bits_min) * 16 + 7) / 8 + 2;
            if (bytes_total < size_estimate) {
                shortcut = 2;
            }
        }
        if (shortcut) {
            // Shortcut
            if (!stream_.put_raw((u8)0xFF)) {
                return false;
            }
            if (shortcut == 1) {
                // compress trailing
                int n = 64 - trailing_bits_min;
                for (int i = 0; i < 16; i++) {
                    diffs[i] >>= trailing_bits_min;
                }
                if (!stream_.put_raw(static_cast<u8>(n) | 0x80)) {
                    return false;
                }
                if (!pack(diffs, n)) {
                    return false;
                }
            } else {
                // compress leading
                int n = 64 - leading_bits_min;
                if (!stream_.put_raw(static_cast<u8>(n))) {
                    return false;
                }
                if (!pack(diffs, n)) {
                    return false;
                }
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

    std::tuple<u64, unsigned char, int, int> encode(double value) {
        union {
            double real;
            u64 bits;
        } curr = {};
        curr.real = value;
        u64 predicted = predictor_.predict_next();
        predictor_.update(curr.bits);
        u64 diff = curr.bits ^ predicted;


        if (diff == 0) {
            // Fast path for 0-diff values.
            // Flags 7 and 15 are interchangeable.
            // If there is 0 trailing zero bytes and 0 leading bytes
            // code will always generate flag 7 so we can use flag 17
            // for something different (like 0 indication)
            return std::make_tuple(0, 0xF, 64, 64);
        }

        // Number of trailing and leading zero-bytes
        int trailing_bits  = __builtin_ctzl(diff);
        int leading_bits   = __builtin_clzl(diff);
        int trailing_bytes = trailing_bits / 8;
        int leading_bytes  = leading_bits / 8;

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
        return std::make_tuple(diff, flag, leading_bits, trailing_bits);
    }

    bool put(double value) {
        u64 diff;
        unsigned char flag;
        int leading, trailing;
        std::tie(diff, flag, leading, trailing) = encode(value);
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
template<class StreamT=VByteStreamReader>
struct FcmStreamReader {
    StreamT&             stream_;
    PredictorT           predictor_;
    u32                  flags_;
    u32                  iter_;
    u32                  ndiffs_;
    u64                  diffs_[16];

    FcmStreamReader(StreamT& stream)
        : stream_(stream)
        , predictor_(PREDICTOR_N)
        , flags_(0)
        , iter_(0)
        , ndiffs_(0)
    {
    }

    static inline u64 decode_value(StreamT& rstream, unsigned char flag) {
        u64 diff = 0ul;
        int nbytes = (flag & 7) + 1;
        for (int i = 0; i < nbytes; i++) {
            u64 delta = rstream.template read_raw<unsigned char>();
            diff |= delta << (i*8);
        }
        int shift_width = (64 - nbytes*8)*(flag >> 3);
        diff <<= shift_width;
        return diff;
    }

    template <typename T>
    void _unpackN(u64* input, int shift) {
        for (int i = 0; i < 16; i++) {
            T val = static_cast<T>(stream_.template read_raw<T>());
            input[i] |= static_cast<u64>(val) << shift;
        }
    }

    void _unpack1(u64* output, int shift) {
        u16 bits = stream_.template read_raw<u16>();
        for (int i = 0; i < 16; i++) {
            output[i] |= static_cast<u64>((bits & (1 << i)) >> i) << shift;
        }
    }

    void _unpack2(u64* output, int shift) {
        u32 bits = stream_.template read_raw<u32>();
        for (u32 i = 0; i < 16; i++) {
            output[i] |= static_cast<u64>((bits & (3u << 2*i)) >> 2*i) << shift;
        }
    }

    void _unpack3(u64* output, int shift) {
        u64 bits0  = stream_.template read_raw<u32>();
        u64 bits1  = stream_.template read_raw<u16>();
        output[0]  |= ((bits0 & 7)) << shift;
        output[1]  |= ((bits0 & (7u <<  3)) >> 3)  << shift;
        output[2]  |= ((bits0 & (7u <<  6)) >> 6)  << shift;
        output[3]  |= ((bits0 & (7u <<  9)) >> 9)  << shift;
        output[4]  |= ((bits0 & (7u << 12)) >> 12) << shift;
        output[5]  |= ((bits0 & (7u << 15)) >> 15) << shift;
        output[6]  |= ((bits0 & (7u << 18)) >> 18) << shift;
        output[7]  |= ((bits0 & (7u << 21)) >> 21) << shift;
        output[8]  |= ((bits0 & (7u << 24)) >> 24) << shift;
        output[9]  |= ((bits0 & (7u << 27)) >> 27) << shift;
        output[10] |= (((bits0 & (3u << 30)) >> 30) | ((bits1 & 1u) << 2)) << shift;
        output[11] |= ((bits1 & (7u <<  1)) >> 1)  << shift;
        output[12] |= ((bits1 & (7u <<  4)) >> 4)  << shift;
        output[13] |= ((bits1 & (7u <<  7)) >> 7)  << shift;
        output[14] |= ((bits1 & (7u << 10)) >> 10) << shift;
        output[15] |= ((bits1 & (7u << 13)) >> 13) << shift;
    }

    void _unpack4(u64* output, int shift) {
        u64 bits0  = stream_.template read_raw<u64>();
        output[0]  |= ((bits0 & 0xF)) << shift;
        output[1]  |= ((bits0 & (15ull <<  4)) >>  4)  << shift;
        output[2]  |= ((bits0 & (15ull <<  8)) >>  8)  << shift;
        output[3]  |= ((bits0 & (15ull << 12)) >> 12)  << shift;
        output[4]  |= ((bits0 & (15ull << 16)) >> 16)  << shift;
        output[5]  |= ((bits0 & (15ull << 20)) >> 20)  << shift;
        output[6]  |= ((bits0 & (15ull << 24)) >> 24)  << shift;
        output[7]  |= ((bits0 & (15ull << 28)) >> 28)  << shift;
        output[8]  |= ((bits0 & (15ull << 32)) >> 32)  << shift;
        output[9]  |= ((bits0 & (15ull << 36)) >> 36)  << shift;
        output[10] |= ((bits0 & (15ull << 40)) >> 40)  << shift;
        output[11] |= ((bits0 & (15ull << 44)) >> 44)  << shift;
        output[12] |= ((bits0 & (15ull << 48)) >> 48)  << shift;
        output[13] |= ((bits0 & (15ull << 52)) >> 52)  << shift;
        output[14] |= ((bits0 & (15ull << 56)) >> 56)  << shift;
        output[15] |= ((bits0 & (15ull << 60)) >> 60)  << shift;
    }

    void _unpack5(u64* output, int shift) {
        u64 bits0  = stream_.template read_raw<u64>();
        u64 bits1  = stream_.template read_raw<u16>();
        output[0]  |= ((bits0 & 0x1F)) << shift;
        output[1]  |= ((bits0 & (0x1Full <<  5)) >>  5) << shift;
        output[2]  |= ((bits0 & (0x1Full << 10)) >> 10) << shift;
        output[3]  |= ((bits0 & (0x1Full << 15)) >> 15) << shift;
        output[4]  |= ((bits0 & (0x1Full << 20)) >> 20) << shift;
        output[5]  |= ((bits0 & (0x1Full << 25)) >> 25) << shift;
        output[6]  |= ((bits0 & (0x1Full << 30)) >> 30) << shift;
        output[7]  |= ((bits0 & (0x1Full << 35)) >> 35) << shift;
        output[8]  |= ((bits0 & (0x1Full << 40)) >> 40) << shift;
        output[9]  |= ((bits0 & (0x1Full << 45)) >> 45) << shift;
        output[10] |= ((bits0 & (0x1Full << 50)) >> 50) << shift;
        output[11] |= ((bits0 & (0x1Full << 55)) >> 55) << shift;
        output[12] |= (((bits0 & (0x0Full << 60)) >> 60) | ((bits1 & 1) << 4)) << shift;
        output[13] |= ((bits1 & (0x1Full <<  1)) >>  1) << shift;
        output[14] |= ((bits1 & (0x1Full <<  6)) >>  6) << shift;
        output[15] |= ((bits1 & (0x1Full << 11)) >> 11) << shift;
    }

    void _unpack6(u64* output, int shift) {
        u64 bits0  = stream_.template read_raw<u64>();
        u64 bits1  = stream_.template read_raw<u32>();
        output[0]  |= ((bits0 & 0x3F)) << shift;
        output[1]  |= ((bits0 & (0x3Full <<  6)) >>  6) << shift;
        output[2]  |= ((bits0 & (0x3Full << 12)) >> 12) << shift;
        output[3]  |= ((bits0 & (0x3Full << 18)) >> 18) << shift;
        output[4]  |= ((bits0 & (0x3Full << 24)) >> 24) << shift;
        output[5]  |= ((bits0 & (0x3Full << 30)) >> 30) << shift;
        output[6]  |= ((bits0 & (0x3Full << 36)) >> 36) << shift;
        output[7]  |= ((bits0 & (0x3Full << 42)) >> 42) << shift;
        output[8]  |= ((bits0 & (0x3Full << 48)) >> 48) << shift;
        output[9]  |= ((bits0 & (0x3Full << 54)) >> 54) << shift;
        output[10] |= (((bits0 & (0xFull << 60)) >> 60) | (bits1 & 0x3) << 4) << shift;
        output[11] |= ((bits1 & (0x3Full <<  2)) >>  2) << shift;
        output[12] |= ((bits1 & (0x3Full <<  8)) >>  8) << shift;
        output[13] |= ((bits1 & (0x3Full << 14)) >> 14) << shift;
        output[14] |= ((bits1 & (0x3Full << 20)) >> 20) << shift;
        output[15] |= ((bits1 & (0x3Full << 26)) >> 26) << shift;
    }

    void _unpack7(u64* output, int shift) {
        u64 bits0  = stream_.template read_raw<u64>();
        u64 bits1  = stream_.template read_raw<u32>();
        u64 bits2  = stream_.template read_raw<u16>();
        output[0]  |= ((bits0 & 0x7F)) << shift;
        output[1]  |= ((bits0 & (0x7Full <<  7)) >>  7) << shift;
        output[2]  |= ((bits0 & (0x7Full << 14)) >> 14) << shift;
        output[3]  |= ((bits0 & (0x7Full << 21)) >> 21) << shift;
        output[4]  |= ((bits0 & (0x7Full << 28)) >> 28) << shift;
        output[5]  |= ((bits0 & (0x7Full << 35)) >> 35) << shift;
        output[6]  |= ((bits0 & (0x7Full << 42)) >> 42) << shift;
        output[7]  |= ((bits0 & (0x7Full << 49)) >> 49) << shift;
        output[8]  |= ((bits0 & (0x7Full << 56)) >> 56) << shift;
        output[9]  |= (((bits0 & (0x01ull << 63)) >> 63) | ((bits1 & 0x3F) << 1)) << shift;
        output[10] |= ((bits1 & (0x7Full <<  6)) >>  6) << shift;
        output[11] |= ((bits1 & (0x7Full << 13)) >> 13) << shift;
        output[12] |= ((bits1 & (0x7Full << 20)) >> 20) << shift;
        output[13] |= (((bits1 & (0x1Full << 27)) >> 27) | ((bits2 & 0x03) << 5)) << shift;
        output[14] |= ((bits2 & (0x7Full <<  2)) >>  2) << shift;
        output[15] |= ((bits2 & (0x7Full <<  9)) >>  9) << shift;
    }


    void unpack(u64* output, int n) {
        switch(n) {
        case 0:
            break;
        case 1:
            _unpack1(output, 0);
            break;
        case 2:
            _unpack2(output, 0);
            break;
        case 3:
            _unpack3(output, 0);
            break;
        case 4:
            _unpack4(output, 0);
            break;
        case 5:
            _unpack5(output, 0);
            break;
        case 6:
            _unpack6(output, 0);
            break;
        case 7:
            _unpack7(output, 0);
            break;
        case 8:
            _unpackN<u8>(output, 0);
            break;
        case 9:
            _unpackN<u8>(output, 0);
            _unpack1(output, 8);
            break;
        case 10:
            _unpackN<u8>(output, 0);
            _unpack2(output, 8);
            break;
        case 11:
            _unpackN<u8>(output, 0);
            _unpack3(output, 8);
            break;
        case 12:
            _unpackN<u8>(output, 0);
            _unpack4(output, 8);
            break;
        case 13:
            _unpackN<u8>(output, 0);
            _unpack5(output, 8);
            break;
        case 14:
            _unpackN<u8>(output, 0);
            _unpack6(output, 8);
            break;
        case 15:
            _unpackN<u8>(output, 0);
            _unpack7(output, 8);
            break;
        case 16:
            _unpackN<u16>(output, 0);
            break;
        case 17:
            _unpackN<u16>(output, 0);
            _unpack1(output, 16);
            break;
        case 18:
            _unpackN<u16>(output, 0);
            _unpack2(output, 16);
            break;
        case 19:
            _unpackN<u16>(output, 0);
            _unpack3(output, 16);
            break;
        case 20:
            _unpackN<u16>(output, 0);
            _unpack4(output, 16);
            break;
        case 21:
            _unpackN<u16>(output, 0);
            _unpack5(output, 16);
            break;
        case 22:
            _unpackN<u16>(output, 0);
            _unpack6(output, 16);
            break;
        case 23:
            _unpackN<u16>(output, 0);
            _unpack7(output, 16);
            break;
        case 24:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            break;
        case 25:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack1(output, 24);
            break;
        case 26:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack2(output, 24);
            break;
        case 27:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack3(output, 24);
            break;
        case 28:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack4(output, 24);
            break;
        case 29:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack5(output, 24);
            break;
        case 30:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack6(output, 24);
            break;
        case 31:
            _unpackN<u16>(output, 0);
            _unpackN<u8>(output, 16);
            _unpack7(output, 24);
            break;
        case 32:
            _unpackN<u32>(output, 0);
            break;
        case 33:
            _unpackN<u32>(output, 0);
            _unpack1(output, 32);
            break;
        case 34:
            _unpackN<u32>(output, 0);
            _unpack2(output, 32);
            break;
        case 35:
            _unpackN<u32>(output, 0);
            _unpack3(output, 32);
            break;
        case 36:
            _unpackN<u32>(output, 0);
            _unpack4(output, 32);
            break;
        case 37:
            _unpackN<u32>(output, 0);
            _unpack5(output, 32);
            break;
        case 38:
            _unpackN<u32>(output, 0);
            _unpack6(output, 32);
            break;
        case 39:
            _unpackN<u32>(output, 0);
            _unpack7(output, 32);
            break;
        case 40:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            break;
        case 41:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack1(output, 40);
            break;
        case 42:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack2(output, 40);
            break;
        case 43:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack3(output, 40);
            break;
        case 44:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack4(output, 40);
            break;
        case 45:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack5(output, 40);
            break;
        case 46:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack6(output, 40);
            break;
        case 47:
            _unpackN<u32>(output, 0);
            _unpackN<u8>(output, 32);
            _unpack7(output, 40);
            break;
        case 48:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            break;
        case 49:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack1(output, 48);
            break;
        case 50:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack2(output, 48);
            break;
        case 51:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack3(output, 48);
            break;
        case 52:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack4(output, 48);
            break;
        case 53:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack5(output, 48);
            break;
        case 54:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack6(output, 48);
            break;
        case 55:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpack7(output, 48);
            break;
        case 56:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            break;
        case 57:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack1(output, 56);
            break;
        case 58:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack2(output, 56);
            break;
        case 59:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack3(output, 56);
            break;
        case 60:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack4(output, 56);
            break;
        case 61:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack5(output, 56);
            break;
        case 62:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack6(output, 56);
            break;
        case 63:
            _unpackN<u32>(output, 0);
            _unpackN<u16>(output, 32);
            _unpackN<u8>(output, 48);
            _unpack7(output, 56);
            break;
        case 64:
            _unpackN<u64>(output, 0);
            break;
        }
    }

    double next() {
        unsigned char flag = 0;
        if (iter_++ % 2 == 0 && ndiffs_ == 0) {
            flags_ = static_cast<u32>(stream_.template read_raw<u8>());
            if (flags_ == 0xFF) {
                // Shortcut
                ndiffs_ = 16;
                u8 code_word = static_cast<int>(stream_.template read_raw<u8>());
                int bit_width = static_cast<int>(code_word) & 0x7F;
                std::fill_n(diffs_, 16, 0);
                unpack(diffs_, bit_width);
                if (code_word & 0x80) {
                    int shift_width = 64  - bit_width;
                    for (int i = 0; i < 16; i++) {
                        diffs_[i] <<= shift_width;
                    }
                }
            }
            flag = static_cast<unsigned char>(flags_ >> 4);
        }
        else {
            flag = static_cast<unsigned char>(flags_ & 0xF);
        }
        u64 diff;
        if (ndiffs_ == 0) {
            diff = decode_value(stream_, flag);
        } else {
            diff = diffs_[16 - ndiffs_];
            ndiffs_--;
        }
        union {
            u64 bits;
            double real;
        } curr = {};
        u64 predicted = predictor_.predict_next();
        curr.bits = predicted ^ diff;
        predictor_.update(curr.bits);
        return curr.real;
    }

    const u8* pos() const { return stream_.pos(); }

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
    FcmStreamReader<>   val_stream_;
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
    IOVecBlockWriter(BlockT* block, u32 offset = 0)
        : stream_(block)
        , ts_stream_(stream_)
        , val_stream_(stream_)
        , write_index_(0)
    {
        if (offset > 0) {
            stream_.skip(offset);
        }
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

/*
 * Compressor helper functions.
 */
namespace {

    inline u16 get_block_version(const u8* pdata) {
        u16 version = *reinterpret_cast<const u16*>(pdata);
        return version;
    }

    inline u32 get_main_size(const u8* pdata) {
        u16 main = *reinterpret_cast<const u16*>(pdata + 2);
        return static_cast<u32>(main) * DataBlockReader::CHUNK_SIZE;
    }

    inline u32 get_total_size(const u8* pdata) {
        u16 main = *reinterpret_cast<const u16*>(pdata + 2);
        u16 tail = *reinterpret_cast<const u16*>(pdata + 4);
        return tail + static_cast<u32>(main) * DataBlockReader::CHUNK_SIZE;
    }

    inline aku_ParamId get_block_id(const u8* pdata) {
        aku_ParamId id = *reinterpret_cast<const aku_ParamId*>(pdata + 6);
        return id;
    }
}  // end namespace

/**
 * Vectorized decompressor.
 * This class is intended to be used with vector I/O
 * blocks (used by corresponding compressor).
 */
template<class BlockT>
struct IOVecBlockReader {
    enum {
        CHUNK_SIZE = 16,
        CHUNK_MASK = 15,
    };
    typedef IOVecVByteStreamReader<BlockT> StreamT;
    typedef DeltaDeltaStreamReader<16, u64, StreamT> DeltaDeltaReaderT;
    typedef FcmStreamReader<StreamT> FcmStreamReaderT;

    StreamT             stream_;
    DeltaDeltaReaderT   ts_stream_;
    FcmStreamReaderT    val_stream_;
    aku_Timestamp       read_buffer_[CHUNK_SIZE];
    u32                 read_index_;
    const u8*           begin_;

    IOVecBlockReader(const BlockT* block, u32 offset = 0)
        : stream_(block)
        , ts_stream_(stream_)
        , val_stream_(stream_)
        , read_buffer_{}
        , read_index_(0)
    {
        if (offset > 0) {
            stream_.skip(offset);
        }
        begin_ = stream_.skip(DataBlockWriter::HEADER_SIZE);
    }

    std::tuple<aku_Status, aku_Timestamp, double> next() {
        if (read_index_ < get_main_size(begin_)) {
            auto chunk_index = read_index_++ & CHUNK_MASK;
            if (chunk_index == 0) {
                // read all timestamps
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    read_buffer_[i] = ts_stream_.next();
                }
            }
            double value = val_stream_.next();
            return std::make_tuple(AKU_SUCCESS, read_buffer_[chunk_index], value);
        } else {
            // handle tail values
            if (read_index_ < get_total_size(begin_)) {
                read_index_++;
                auto ts = stream_.template read_raw<aku_Timestamp>();
                auto value = stream_.template read_raw<double>();
                return std::make_tuple(AKU_SUCCESS, ts, value);
            }
        }
        return std::make_tuple(AKU_ENO_DATA, 0ull, 0.0);
    }

    size_t nelements() const {
        return get_total_size(begin_);
    }

    aku_ParamId get_id() const {
        return get_block_id(begin_);
    }

    u16 version() const {
        return get_block_version(begin_);
    }
};

}  // namespace V2
}
