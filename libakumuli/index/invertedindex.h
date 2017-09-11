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
#include "akumuli.h"
#include "hashfnfamily.h"
#include "stringpool.h"
#include "util.h"

#include <memory>
#include <unordered_map>
#include <vector>
#include <cassert>
#include <iterator>
#include <algorithm>

namespace Akumuli {

struct TwoUnivHashFnFamily {
    const int        INTERNAL_CARDINALITY_;
    std::vector<u64> a;
    std::vector<u64> b;
    u64              prime;
    u64              modulo;

    TwoUnivHashFnFamily(int cardinality, size_t modulo);

    u64 hash(int ix, u64 value) const;
};

//             //
//  CM-sketch  //
//             //

namespace details {
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
    void put(std::vector<char>& vec) const {
        TVal           value = value_;
        unsigned char p;
        while (true) {
            p = value & 0x7F;
            value >>= 7;
            if (value != 0) {
                p |= 0x80;
                vec.push_back(static_cast<char>(p));
            } else {
                vec.push_back(static_cast<char>(p));
                break;
            }
        }
    }

    //! turn into integer
    operator TVal() const { return value_; }
};

//! Base128 encoder
struct Base128StreamWriter {
    // underlying memory region
    std::vector<char>* buffer_;

    Base128StreamWriter(std::vector<char>& buffer)
        : buffer_(&buffer)
    {}

    Base128StreamWriter(Base128StreamWriter const& other)
        : buffer_(other.buffer_)
    {}

    Base128StreamWriter& operator = (Base128StreamWriter const& other) {
        if (&other == this) {
            return *this;
        }
        buffer_ = other.buffer_;
        return *this;
    }

    void reset(std::vector<char>& buffer) {
        buffer_ = &buffer;
    }

    bool empty() const { return buffer_->empty(); }

    //! Put value into stream.
    template <class TVal> bool put(TVal value) {
        Base128Int<TVal> val(value);
        val.put(*buffer_);
        return true;
    }
};

//! Base128 decoder
struct Base128StreamReader {
    const unsigned char* pos_;
    const unsigned char* end_;

    Base128StreamReader(const unsigned char* begin, const unsigned char* end)
        : pos_(begin)
        , end_(end) {}

    Base128StreamReader(Base128StreamReader const& other)
        : pos_(other.pos_)
        , end_(other.end_)
    {
    }

    Base128StreamReader& operator = (Base128StreamReader const& other) {
        if (&other == this) {
            return *this;
        }
        pos_ = other.pos_;
        end_ = other.end_;
        return *this;
    }

    template <class TVal> TVal next() {
        Base128Int<TVal> value;
        auto             p = value.get(pos_, end_);
        if (p == pos_) {
            AKU_PANIC("Base128Stream read error");
        }
        pos_ = p;
        return static_cast<TVal>(value);
    }
};

template <class Stream, typename TVal> struct DeltaStreamWriter {
    Stream* stream_;
    TVal   prev_;

    template<class Substream>
    DeltaStreamWriter(Substream& stream)
        : stream_(&stream)
        , prev_{}
    {}

    DeltaStreamWriter(DeltaStreamWriter const& other)
        : stream_(other.stream_)
        , prev_(other.prev_)
    {
    }

    DeltaStreamWriter& operator = (DeltaStreamWriter const& other) {
        if (this == &other) {
            return *this;
        }
        stream_ = other.stream_;
        prev_   = other.prev_;
        return *this;
    }

    bool put(TVal value) {
        auto result = stream_->put(static_cast<TVal>(value) - prev_);
        prev_       = value;
        return result;
    }
};


template <class Stream, typename TVal> struct DeltaStreamReader {
    Stream* stream_;
    TVal   prev_;

    DeltaStreamReader(Stream& stream)
        : stream_(&stream)
        , prev_() {}

    DeltaStreamReader(DeltaStreamReader const& other)
        : stream_(other.stream_)
        , prev_(other.prev_)
    {
    }

    DeltaStreamReader& operator = (DeltaStreamReader const& other) {
        if (&other == this) {
            return *this;
        }
        stream_ = other.stream_;
        prev_   = other.prev_;
        return *this;
    }

    TVal next() {
        TVal delta = stream_->template next<TVal>();
        TVal value = prev_ + delta;
        prev_      = value;
        return value;
    }
};

}

// Iterator for compressed PList

class CompressedPListConstIterator {
    size_t card_;
    details::Base128StreamReader reader_;
    details::DeltaStreamReader<details::Base128StreamReader, u64> delta_;
    size_t pos_;
    u64 curr_;
public:
    typedef u64 value_type;

    CompressedPListConstIterator(std::vector<char> const& vec, size_t c);

    /**
     * @brief Create iterator pointing to the end of the sequence
     */
    CompressedPListConstIterator(std::vector<char> const& vec, size_t c, bool);

    CompressedPListConstIterator(CompressedPListConstIterator const& other);

    CompressedPListConstIterator& operator = (CompressedPListConstIterator const& other);

    u64 operator * () const;

    CompressedPListConstIterator& operator ++ ();

    bool operator == (CompressedPListConstIterator const& other) const;

    bool operator != (CompressedPListConstIterator const& other) const;
};

}  // namespace Akumuli

namespace std {
    template<>
    struct iterator_traits<Akumuli::CompressedPListConstIterator> {
        typedef u64 value_type;
        typedef forward_iterator_tag iterator_category;
    };
}

namespace Akumuli {

/**
 * Compressed postings list
 */
class CompressedPList {
    std::vector<char> buffer_;
    details::Base128StreamWriter writer_;
    details::DeltaStreamWriter<details::Base128StreamWriter, u64> delta_;
    size_t cardinality_;
    bool moved_;
public:

    typedef u64 value_type;

    CompressedPList();

    CompressedPList(CompressedPList const& other);

    CompressedPList& operator = (CompressedPList && other);

    CompressedPList(CompressedPList && other);

    CompressedPList& operator = (CompressedPList const& other) = delete;

    void add(u64 x);

    void push_back(u64 x);

    size_t getSizeInBytes() const;

    size_t cardinality() const;

    CompressedPList operator & (CompressedPList const& other) const;

    CompressedPList operator | (CompressedPList const& other) const;

    CompressedPList operator ^ (CompressedPList const& other) const;

    CompressedPListConstIterator begin() const;

    CompressedPListConstIterator end() const;
};


//            //
//  CMSketch  //
//            //

class CMSketch {
    typedef CompressedPList TVal;
    std::vector<std::vector<TVal>> table_;
    const u32 N;
    const u32 M;
    const u32 mask_;
    const u32 bits_;

    inline u32 extracthash(u64 key, u32 i) const {
        u32 hash = (key >> (i * bits_)) & mask_;
        return hash;
    }
public:
    CMSketch(u32 M);

    void add(u64 key, u64 value);

    size_t get_size_in_bytes() const;

    TVal extract(u64 value) const;
};


//              //
//  MetricName  //
//              //

class MetricName {
    std::string name_;
public:
    MetricName(const char* begin, const char* end);

    MetricName(const char* str);

    StringT get_value() const;

    bool check(const char* begin, const char* end) const;
};


//                //
//  TagValuePair  //
//                //

/**
 * @brief Tag value pair
 */
class TagValuePair {
    std::string value_;  //! Value that holds both tag and value
public:
    TagValuePair(const char* begin, const char* end);

    TagValuePair(const char* str);

    StringT get_value() const;

    bool check(const char* begin, const char* end) const;
};


//                             //
//  IndexQueryResultsIterator  //
//                             //

/**
 * Iterates through query results.
 * This is a pretty minimal implementation, only one ++ operator
 * variant is implemented and no move semantics and traits.
 * It works with set algorithms but shouldn't work with all
 * std algorithms in general.
 */
class IndexQueryResultsIterator {
    CompressedPListConstIterator it_;
    StringPool const* spool_;
public:
    IndexQueryResultsIterator(CompressedPListConstIterator postinglist, StringPool const* spool);

    StringT operator * () const;

    IndexQueryResultsIterator& operator ++ ();

    bool operator == (IndexQueryResultsIterator const& other) const;

    bool operator != (IndexQueryResultsIterator const& other) const;
};


//                     //
//  IndexQueryResults  //
//                     //

class IndexQueryResults {
    CompressedPList postinglist_;
    StringPool const* spool_;
public:
    IndexQueryResults();

    IndexQueryResults(CompressedPList&& plist, StringPool const* spool);

    IndexQueryResults(IndexQueryResults const& other);

    IndexQueryResults& operator = (IndexQueryResults && other);

    IndexQueryResults(IndexQueryResults&& plist);

    template<class Checkable>
    IndexQueryResults filter(std::vector<Checkable> const& values) {
        bool rewrite = false;
        // Check for falce positives
        for (auto it = postinglist_.begin(); it != postinglist_.end(); ++it) {
            auto id = *it;
            auto str = spool_->str(id);
            for (auto const& value: values) {
                if (!value.check(str.first, str.first + str.second)) {
                    rewrite = true;
                    break;
                }
            }
        }
        if (rewrite) {
            // This code only gets triggered when false positives are present
            CompressedPList newplist;
            for (auto it = postinglist_.begin(); it != postinglist_.end(); ++it) {
                auto id = *it;
                auto str = spool_->str(id);
                for (auto const& value: values) {
                    if (value.check(str.first, str.first + str.second)) {
                        newplist.add(id);
                    }
                }
            }
            return IndexQueryResults(std::move(newplist), spool_);
        }
        return *this;
    }

    template<class Checkable>
    IndexQueryResults filter(Checkable const& value) {
        bool rewrite = false;
        // Check for falce positives
        for (auto it = postinglist_.begin(); it != postinglist_.end(); ++it) {
            auto id = *it;
            auto str = spool_->str(id);
            if (!value.check(str.first, str.first + str.second)) {
                rewrite = true;
                break;
            }
        }
        if (rewrite) {
            // This code only gets triggered when false positives are present
            CompressedPList newplist;
            for (auto it = postinglist_.begin(); it != postinglist_.end(); ++it) {
                auto id = *it;
                auto str = spool_->str(id);
                if (value.check(str.first, str.first + str.second)) {
                    newplist.add(id);
                }
            }
            return IndexQueryResults(std::move(newplist), spool_);
        }
        return *this;
    }

    IndexQueryResults intersection(IndexQueryResults const& other);

    IndexQueryResults difference(IndexQueryResults const& other);

    IndexQueryResults join(IndexQueryResults const& other);

    size_t cardinality() const;

    IndexQueryResultsIterator begin() const;

    IndexQueryResultsIterator end() const;
};

}  // namespace
