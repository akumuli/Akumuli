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
#include "invertedindex.h"

#include <random>
#include <memory>
#include <algorithm>

#include "util.h"
#include "stringpool.h"

namespace Akumuli {

//! Move pointer to the of the whitespace, return this pointer or end on error
static const char* skip_space(const char* p, const char* end) {
    while(p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    return p;
}

//! Move pointer to the beginning of the next tag, return this pointer or end on error
static const char* skip_tag(const char* p, const char* end, bool *error) {
    // skip until '='
    while(p < end && *p != '=' && *p != ' ' && *p != '\t') {
        p++;
    }
    if (p == end || *p != '=') {
        *error = true;
        return end;
    }
    // skip until ' '
    const char* c = p;
    while(c < end && *c != ' ') {
        c++;
    }
    *error = c == p;
    return c;
}

static bool write_tags(const char* begin, const char* end, CMSketch* dest_sketch, u64 id) {
    const char* tag_begin = begin;
    const char* tag_end = begin;
    bool err = false;
    while(!err && tag_begin != end) {
        tag_begin = skip_space(tag_begin, end);
        tag_end = tag_begin;
        tag_end = skip_tag(tag_end, end, &err);
        auto tagpair = std::make_pair(tag_begin, static_cast<u32>(tag_end - tag_begin));
        u64 hash = StringTools::hash(tagpair);
        dest_sketch->add(hash, id);
        tag_begin = tag_end;
    }
    return err;
}

static StringT skip_metric_name(const char* begin, const char* end) {
    const char* p = begin;
    // skip metric name
    p = skip_space(p, end);
    if (p == end) {
        return std::make_pair(nullptr, 0);
    }
    const char* m = p;
    while(*p != ' ') {
        p++;
    }
    return std::make_pair(m, p - m);
}

TwoUnivHashFnFamily::TwoUnivHashFnFamily(int cardinality, size_t modulo)
    : INTERNAL_CARDINALITY_(cardinality)
    , prime(2147483647)  // 2^31-1
    , modulo(modulo)
{
    a.resize(INTERNAL_CARDINALITY_);
    b.resize(INTERNAL_CARDINALITY_);
    std::random_device randdev;
    std::minstd_rand generator(randdev());
    std::uniform_int_distribution<> distribution;
    for (int i = 0; i < INTERNAL_CARDINALITY_; i++) {
        a[i] = distribution(generator);
        b[i] = distribution(generator);
    }
}

u64 TwoUnivHashFnFamily::hash(int ix, u64 value) const {
    return ((a[ix]*value + b[ix]) % prime) % modulo;
}


//                           //
//  CompressedPListIterator  //
//                           //

CompressedPListConstIterator::CompressedPListConstIterator(std::vector<char> const& vec, size_t c)
    : card_(c)
    , reader_(reinterpret_cast<const unsigned char*>(vec.data()),
              reinterpret_cast<const unsigned char*>(vec.data() + vec.size()))
    , delta_(reader_)
    , pos_(0)
{
    if (pos_ < card_) {
        curr_ = delta_.next();
    }
}

/**
 * @brief Create iterator pointing to the end of the sequence
 */
CompressedPListConstIterator::CompressedPListConstIterator(std::vector<char> const& vec, size_t c, bool)
    : card_(c)
    , reader_(reinterpret_cast<const unsigned char*>(vec.data()),
              reinterpret_cast<const unsigned char*>(vec.data() + vec.size()))
    , delta_(reader_)
    , pos_(c)
    , curr_()
{
}

CompressedPListConstIterator::CompressedPListConstIterator(CompressedPListConstIterator const& other)
    : card_(other.card_)
    , reader_(other.reader_)
    , delta_(other.delta_)
    , pos_(other.pos_)
    , curr_(other.curr_)
{
}

CompressedPListConstIterator& CompressedPListConstIterator::operator = (CompressedPListConstIterator const& other) {
    if (this == &other) {
        return *this;
    }
    card_ = other.card_;
    reader_ = other.reader_;
    delta_ = other.delta_;
    pos_ = other.pos_;
    curr_ = other.curr_;
    return *this;
}

u64 CompressedPListConstIterator::operator * () const {
    return curr_;
}

CompressedPListConstIterator& CompressedPListConstIterator::operator ++ () {
    pos_++;
    if (pos_ < card_) {
        curr_ = delta_.next();
    }
    return *this;
}

bool CompressedPListConstIterator::operator == (CompressedPListConstIterator const& other) const {
    return pos_ == other.pos_;
}

bool CompressedPListConstIterator::operator != (CompressedPListConstIterator const& other) const {
    return pos_ != other.pos_;
}


//                   //
//  CompressedPList  //
//                   //

CompressedPList::CompressedPList()
    : writer_(buffer_)
    , delta_(writer_)
    , cardinality_(0)
    , moved_(false)
{
}

CompressedPList::CompressedPList(CompressedPList const& other)
    : buffer_(other.buffer_)
    , writer_(buffer_)
    , delta_(writer_)
    , cardinality_(other.cardinality_)
    , moved_(false)
{
    assert(!other.moved_);
}

CompressedPList& CompressedPList::operator = (CompressedPList && other) {
    assert(!other.moved_);
    if (this == &other) {
        return *this;
    }
    other.moved_ = true;
    buffer_.swap(other.buffer_);
    // we don't need to assign writer_ since it contains pointer to buffer_
    // already
    // delta already contain correct pointer to writer_ we only need to
    // update prev_ field
    delta_.prev_ = other.delta_.prev_;
    cardinality_ = other.cardinality_;
    return *this;
}

CompressedPList::CompressedPList(CompressedPList && other)
    : buffer_(std::move(other.buffer_))
    , writer_(buffer_)
    , delta_(writer_)
    , cardinality_(other.cardinality_)
    , moved_(false)
{
    assert(!other.moved_);
    other.moved_ = true;
}

void CompressedPList::add(u64 x) {
    assert(!moved_);
    delta_.put(x);
    cardinality_++;
}

void CompressedPList::push_back(u64 x) {
    assert(!moved_);
    add(x);
}

size_t CompressedPList::getSizeInBytes() const {
    assert(!moved_);
    return buffer_.capacity();
}

size_t CompressedPList::cardinality() const {
    assert(!moved_);
    return cardinality_;
}

CompressedPList CompressedPList::operator & (CompressedPList const& other) const {
    assert(!moved_);
    CompressedPList result;
    std::set_intersection(begin(), end(), other.begin(), other.end(),
                          std::back_inserter(result));
    return result;
}

CompressedPList CompressedPList::operator | (CompressedPList const& other) const {
    assert(!moved_);
    CompressedPList result;
    std::set_union(begin(), end(), other.begin(), other.end(),
                   std::back_inserter(result));
    return result;
}

CompressedPList CompressedPList::operator ^ (CompressedPList const& other) const {
    assert(!moved_);
    CompressedPList result;
    std::set_difference(begin(), end(), other.begin(), other.end(),
                        std::back_inserter(result));
    return result;
}

CompressedPListConstIterator CompressedPList::begin() const {
    assert(!moved_);
    return CompressedPListConstIterator(buffer_, cardinality_);
}

CompressedPListConstIterator CompressedPList::end() const {
    assert(!moved_);
    return CompressedPListConstIterator(buffer_, cardinality_, false);
}

//  CMSketch  //

CMSketch::CMSketch(u32 M)
    : N(3)
    , M(M)
    , mask_(M-1)
    , bits_(static_cast<u32>(log2(static_cast<i64>(mask_))))
{
    // M should be a power of two
    if ((mask_&M) != 0) {
        std::runtime_error err("invalid argument K (should be a power of two)");
        throw err;
    }
    table_.resize(N);
    for (auto& row: table_) {
        row.resize(M);
    }
}

void CMSketch::add(u64 key, u64 value) {
    for (u32 i = 0; i < N; i++) {
        // calculate hash from id to K
        u32 hash = extracthash(key, i);
        table_[i][hash].add(value);
    }
}

size_t CMSketch::get_size_in_bytes() const {
    size_t sum = 0;
    for (auto const& row: table_) {
        for (auto const& bmp: row) {
            sum += bmp.getSizeInBytes();
        }
    }
    return sum;
}

CMSketch::TVal CMSketch::extract(u64 value) const {
    std::vector<const TVal*> inputs;
    for (u32 i = 0; i < N; i++) {
        // calculate hash from id to K
        u32 hash = extracthash(value, i);
        inputs.push_back(&table_[i][hash]);
    }
    return *inputs[0] & *inputs[1] & *inputs[2];
}


//              //
//  MetricName  //
//              //

MetricName::MetricName(const char* begin, const char* end)
    : name_(begin, end)
{
}

MetricName::MetricName(const char* str)
    : name_(str)
{
}

StringT MetricName::get_value() const {
    return std::make_pair(name_.data(), name_.size());
}

bool MetricName::check(const char* begin, const char* end) const {
    auto name = skip_metric_name(begin, end);
    if (name.second == 0) {
        return false;
    }
    // compare
    bool eq = std::equal(name.first, name.first + name.second, name_.begin(), name_.end());
    if (eq) {
        return true;
    }
    return false;
}


//                //
//  TagValuePair  //
//                //

TagValuePair::TagValuePair(const char* begin, const char* end)
    : value_(begin, end)
{
}

TagValuePair::TagValuePair(const char* str)
    : value_(str)
{
}

StringT TagValuePair::get_value() const {
    return std::make_pair(value_.data(), value_.size());
}

bool TagValuePair::check(const char* begin, const char* end) const {
    const char* p = begin;
    // skip metric name
    p = skip_space(p, end);
    if (p == end) {
        return false;
    }
    while(*p != ' ') {
        p++;
    }
    p = skip_space(p, end);
    if (p == end) {
        return false;
    }
    // Check tags
    bool error = false;
    while (!error && p < end) {
        const char* tag_start = p;
        const char* tag_end = skip_tag(tag_start, end, &error);
        bool eq = std::equal(tag_start, tag_end, value_.begin(), value_.end());
        if (eq) {
            return true;
        }
        p = skip_space(tag_end, end);
    }
    return false;
}


//                             //
//  IndexQueryResultsIterator  //
//                             //

IndexQueryResultsIterator::IndexQueryResultsIterator(CompressedPListConstIterator postinglist, StringPool const* spool)
    : it_(postinglist)
    , spool_(spool)
{
}

StringT IndexQueryResultsIterator::operator * () const {
    auto id = *it_;
    auto str = spool_->str(id);
    return str;
}

IndexQueryResultsIterator& IndexQueryResultsIterator::operator ++ () {
    ++it_;
    return *this;
}

bool IndexQueryResultsIterator::operator == (IndexQueryResultsIterator const& other) const {
    return it_ == other.it_;
}

bool IndexQueryResultsIterator::operator != (IndexQueryResultsIterator const& other) const {
    return it_ != other.it_;
}


//                     //
//  IndexQueryResults  //
//                     //

IndexQueryResults::IndexQueryResults()
    : spool_(nullptr)
{}

IndexQueryResults::IndexQueryResults(CompressedPList&& plist, StringPool const* spool)
    : postinglist_(plist)
    , spool_(spool)
{
}

IndexQueryResults::IndexQueryResults(IndexQueryResults const& other)
    : postinglist_(other.postinglist_)
    , spool_(other.spool_)
{
}

IndexQueryResults& IndexQueryResults::operator = (IndexQueryResults && other) {
    if (this == &other) {
        return *this;
    }
    postinglist_ = std::move(other.postinglist_);
    spool_ = other.spool_;
    return *this;
}

IndexQueryResults::IndexQueryResults(IndexQueryResults&& plist)
    : postinglist_(std::move(plist.postinglist_))
    , spool_(plist.spool_)
{
}

IndexQueryResults IndexQueryResults::intersection(IndexQueryResults const& other) {
    if (spool_ == nullptr) {
        spool_ = other.spool_;
    }
    IndexQueryResults result(postinglist_ & other.postinglist_, spool_);
    return result;
}

IndexQueryResults IndexQueryResults::difference(IndexQueryResults const& other) {
    if (spool_ == nullptr) {
        spool_ = other.spool_;
    }
    IndexQueryResults result(postinglist_ ^ other.postinglist_, spool_);
    return result;
}

IndexQueryResults IndexQueryResults::join(IndexQueryResults const& other) {
    if (spool_ == nullptr) {
        spool_ = other.spool_;
    }
    IndexQueryResults result(postinglist_ | other.postinglist_, spool_);
    return result;
}

size_t IndexQueryResults::cardinality() const {
    return postinglist_.cardinality();
}

IndexQueryResultsIterator IndexQueryResults::begin() const {
    return IndexQueryResultsIterator(postinglist_.begin(), spool_);
}

IndexQueryResultsIterator IndexQueryResults::end() const {
    return IndexQueryResultsIterator(postinglist_.end(), spool_);
}

}  // namespace
