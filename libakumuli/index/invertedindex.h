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
#include <map>
#include <sstream>

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

namespace {

inline StringT tostrt(const char* p) {
    return std::make_pair(p, strlen(p));
}

inline StringT tostrt(std::string const& s) {
    return std::make_pair(s.data(), s.size());
}

inline std::string fromstrt(StringT s) {
    return std::string(s.first, s.first + s.second);
}

}

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
        TVal value = value_;
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

    Base128StreamWriter(Base128StreamWriter && other)
        : buffer_(other.buffer_)
    {}

    Base128StreamWriter& operator = (Base128StreamWriter const& other) {
        if (&other == this) {
            return *this;
        }
        buffer_ = other.buffer_;
        return *this;
    }

    Base128StreamWriter& operator = (Base128StreamWriter && other) {
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
    bool put(u64 value) {
        Base128Int<u64> val(value);
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

    Base128StreamReader(Base128StreamReader && other)
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

    Base128StreamReader& operator = (Base128StreamReader && other) {
        if (&other == this) {
            return *this;
        }
        pos_ = other.pos_;
        end_ = other.end_;
        return *this;
    }

    u64 next() {
        Base128Int<u64> value;
        auto p = value.get(pos_, end_);
        if (p == pos_) {
            AKU_PANIC("Base128Stream read error");
        }
        pos_ = p;
        return static_cast<u64>(value);
    }
};

struct DeltaStreamWriter {
    Base128StreamWriter* stream_;
    u64 prev_;

    DeltaStreamWriter(Base128StreamWriter& stream)
        : stream_(&stream)
        , prev_(0)
    {}

    DeltaStreamWriter(DeltaStreamWriter const& other)
        : stream_(other.stream_)
        , prev_(other.prev_)
    {
    }

    DeltaStreamWriter(DeltaStreamWriter && other)
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

    DeltaStreamWriter& operator = (DeltaStreamWriter && other) {
        if (this == &other) {
            return *this;
        }
        stream_ = other.stream_;
        prev_   = other.prev_;
        return *this;
    }

    bool put(u64 value) {
        auto result = stream_->put(value - prev_);
        prev_       = value;
        return result;
    }
};


struct DeltaStreamReader {
    Base128StreamReader* stream_;
    u64 prev_;

    DeltaStreamReader(Base128StreamReader& stream)
        : stream_(&stream)
        , prev_(0)
    {
    }

    DeltaStreamReader(Base128StreamReader& stream, DeltaStreamReader const& other)
        : stream_(&stream)
        , prev_(other.prev_)
    {
    }

    DeltaStreamReader(DeltaStreamReader const& other)
        : stream_(other.stream_)
        , prev_(other.prev_)
    {
    }

    DeltaStreamReader(DeltaStreamReader && other)
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

    DeltaStreamReader& operator = (DeltaStreamReader && other) {
        if (&other == this) {
            return *this;
        }
        stream_ = other.stream_;
        prev_   = other.prev_;
        return *this;
    }

    u64 next() {
        u64 delta = stream_->next();
        u64 value = prev_ + delta;
        prev_      = value;
        return value;
    }
};

}

// Iterator for compressed PList

class CompressedPListConstIterator {
    size_t card_;
    details::Base128StreamReader reader_;
    details::DeltaStreamReader delta_;
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
    details::DeltaStreamWriter delta_;
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

    CompressedPList unique() const;

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


//               //
// Inverted Index //
//               //

class InvertedIndex {
    typedef CompressedPList TVal;
    std::unordered_map<u64, TVal> table_;
public:
    InvertedIndex(u32);

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

    TagValuePair(std::string str);

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
    IndexQueryResults filter(std::vector<Checkable> const& values) const {
        bool rewrite = false;
        // Check for falce positives
        for (auto it = postinglist_.begin(); it != postinglist_.end(); ++it) {
            auto id = *it;
            auto str = spool_->str(id);
            bool success = false;
            for (auto const& value: values) {
                if (value.check(str.first, str.first + str.second)) {
                    success = true;
                    break;
                }
            }
            if (!success) {
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
                for (auto const& value: values) {
                    bool add = false;
                    if (value.check(str.first, str.first + str.second)) {
                        add = true;
                        break;
                    }
                    if (add) {
                        newplist.add(id);
                    }
                }
            }
            return IndexQueryResults(std::move(newplist), spool_);
        }
        return *this;
    }

    template<class Checkable>
    IndexQueryResults filter(Checkable const& value) const {
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

    IndexQueryResults unique() const;

    IndexQueryResults intersection(IndexQueryResults const& other) const;

    IndexQueryResults difference(IndexQueryResults const& other) const;

    IndexQueryResults join(IndexQueryResults const& other) const;

    size_t cardinality() const;

    IndexQueryResultsIterator begin() const;

    IndexQueryResultsIterator end() const;
};


//             //
//  IndexBase  //
//             //

struct IndexBase {
    virtual ~IndexBase() = default;
    virtual IndexQueryResults tagvalue_query(TagValuePair const& value) const = 0;
    virtual IndexQueryResults metric_query(MetricName const& value) const = 0;
    virtual std::vector<StringT> list_metric_names() const = 0;
    virtual std::vector<StringT> list_tags(StringT metric) const = 0;
    virtual std::vector<StringT> list_tag_values(StringT metric, StringT tag) const = 0;
};


//                      //
//  IndexQueryNodeBase  //
//                      //

class IndexQueryNodeBase {
    const char* const name_;

public:

    /**
     * @brief IndexQueryNodeBase c-tor
     * @param name is a static string that contains node name (used for introspection)
     */
    IndexQueryNodeBase(const char* name)
        : name_(name)
    {
    }

    virtual ~IndexQueryNodeBase() = default;

    virtual IndexQueryResults query(const IndexBase&) const = 0;

    const char* get_name() const {
        return name_;
    }
};


//               //
//  IncludeTags  //
//               //

/**
 * Extracts only series that have all specified tag-value
 * combinations.
 */
struct IncludeIfAllTagsMatch : IndexQueryNodeBase {
    constexpr static const char* node_name_ = "include-tags";
    MetricName metric_;
    std::vector<TagValuePair> pairs_;

    template<class Iter>
    IncludeIfAllTagsMatch(MetricName const& metric, Iter begin, Iter end)
        : IndexQueryNodeBase(node_name_)
        , metric_(metric)
        , pairs_(begin, end)
    {
    }

    virtual IndexQueryResults query(IndexBase const&) const;
};


//                    //
//  IncludeMany2Many  //
//                    //

struct IncludeMany2Many : IndexQueryNodeBase {
    constexpr static const char* node_name_ = "many2many";
    MetricName metric_;
    std::map<std::string, std::vector<std::string>> tags_;

    IncludeMany2Many(std::string mname, std::map<std::string, std::vector<std::string>> const& map);

    virtual IndexQueryResults query(IndexBase const& index) const;
};

//                   //
//  IncludeIfHasTag  //
//                   //

/**
 * Extracts only series that have specified tag-value
 * combinations.
 */
struct IncludeIfHasTag : IndexQueryNodeBase {
    constexpr static const char* node_name_ = "include-if-has-tag";
    std::string metric_;
    std::vector<std::string> tagnames_;

    template<class Vec>
    IncludeIfHasTag(std::string const& metric, Vec && tags)
        : IndexQueryNodeBase(node_name_)
        , metric_(metric)
        , tagnames_(std::forward<Vec>(tags))
    {
    }

    template<class FwIt>
    IncludeIfHasTag(std::string const& metric, FwIt begin, FwIt end)
        : IndexQueryNodeBase(node_name_)
        , metric_(metric)
        , tagnames_(begin, end)
    {
    }

    virtual IndexQueryResults query(IndexBase const&) const;
};

//               //
//  ExcludeTags  //
//               //

/**
 * Extracts only series that doesn't have specified tag-value
 * combinations.
 */
struct ExcludeTags : IndexQueryNodeBase {
    constexpr static const char* node_name_ = "exclude-tags";
    MetricName metric_;
    std::vector<TagValuePair> pairs_;

    template<class Iter>
    ExcludeTags(MetricName const& metric, Iter begin, Iter end)
        : IndexQueryNodeBase(node_name_)
        , metric_(metric)
        , pairs_(begin, end)
    {
    }

    virtual IndexQueryResults query(IndexBase const&) const;
};


//              //
//  JoinByTags  //
//              //

struct JoinByTags : IndexQueryNodeBase {
    constexpr static const char* node_name_ = "join-by-tags";
    std::vector<MetricName> metrics_;
    std::vector<TagValuePair> pairs_;

    template<class MIter, class TIter>
    JoinByTags(MIter mbegin, MIter mend, TIter tbegin, TIter tend)
        : IndexQueryNodeBase(node_name_)
        , metrics_(mbegin, mend)
        , pairs_(tbegin, tend)
    {
    }

    virtual IndexQueryResults query(IndexBase const&) const;
};


//                      //
//  SeriesNameTopology  //
//                      //

class SeriesNameTopology {
    typedef StringTools::L3TableT IndexT;
    IndexT index_;
public:
    SeriesNameTopology();

    void add_name(StringT name);

    std::vector<StringT> list_metric_names() const;

    std::vector<StringT> list_tags(StringT metric) const;

    std::vector<StringT> list_tag_values(StringT metric, StringT tag) const;
};


//         //
//  Index  //
//         //

class Index : public IndexBase {
    StringPool pool_;
    StringTools::TableT table_;
    //CMSketch metrics_names_;
    //CMSketch tagvalue_pairs_;
    InvertedIndex metrics_names_;
    InvertedIndex tagvalue_pairs_;
    SeriesNameTopology topology_;
public:
    Index();

    SeriesNameTopology const& get_topology() const;

    size_t cardinality() const;

    size_t memory_use() const;

    size_t index_memory_use() const;

    size_t pool_memory_use() const;

    /**
     * @brief Add new string to index
     * @return status and resulting string (scope of the string is the same as the scope of the index)
     */
    std::tuple<aku_Status, StringT> append(const char* begin, const char* end);

    virtual IndexQueryResults tagvalue_query(const TagValuePair &value) const;

    virtual IndexQueryResults metric_query(const MetricName &value) const;

    virtual std::vector<StringT> list_metric_names() const;

    virtual std::vector<StringT> list_tags(StringT metric) const;

    virtual std::vector<StringT> list_tag_values(StringT metric, StringT tag) const;
};

}  // namespace
