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

namespace Akumuli {

static const int CARDINALITY = 3;

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

}  // namespace
