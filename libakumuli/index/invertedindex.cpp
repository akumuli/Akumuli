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
#include <sstream>

#include "util.h"
#include "stringpool.h"
#include "seriesparser.h"

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

template<class Table>
bool write_tags(const char* begin, const char* end, Table* dest_sketch, u64 id) {
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

/**
 * @brief Split tag=value pair into tag and value
 * @return true on success, false otherwise
 */
static bool split_pair(StringT pair, StringT* outtag, StringT* outval) {
    const char* p = pair.first;
    const char* end = p + pair.second;
    while (*p != '=' && p < end) {
        p++;
    }
    if (p == end) {
        return false;
    }
    *outtag = std::make_pair(pair.first, p - pair.first);
    *outval = std::make_pair(p + 1, pair.second - (p - pair.first + 1));
    return true;
}

//                       //
//  TwoUnivHashFnFamily  //
//                       //

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
    , delta_(reader_, other.delta_)
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

CompressedPList CompressedPList::unique() const {
    CompressedPList result;
    std::unique_copy(begin(), end(), std::back_inserter(result));
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

//               //
// InvertedIndex //
//               //

InvertedIndex::InvertedIndex(u32) {
}

void InvertedIndex::add(u64 key, u64 value) {
    table_[key].add(value);
}

size_t InvertedIndex::get_size_in_bytes() const {
    size_t sum = 0;
    for (auto const& row: table_) {
        auto const& list = row.second;
        sum += list.getSizeInBytes();
    }
    return sum;
}

InvertedIndex::TVal InvertedIndex::extract(u64 value) const {
    auto it = table_.find(value);
    if (it == table_.end()) {
        // Return empty value
        return TVal();
    }
    return it->second;
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
    if (name.second != name_.size()) {
        return false;
    }
    bool eq = std::equal(name.first, name.first + name.second, name_.begin());
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

TagValuePair::TagValuePair(std::string str)
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
        bool eq = false;
        if (static_cast<size_t>(tag_end - tag_start) == value_.size()) {
            eq = std::equal(tag_start, tag_end, value_.begin());
        }
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


IndexQueryResults IndexQueryResults::unique() const {
    IndexQueryResults result(postinglist_.unique(), spool_);
    return result;
}

IndexQueryResults IndexQueryResults::intersection(IndexQueryResults const& other) const {
    const StringPool *spool = spool_;
    if (spool == nullptr) {
        spool = other.spool_;
    }
    IndexQueryResults result(postinglist_ & other.postinglist_, spool);
    return result;
}

IndexQueryResults IndexQueryResults::difference(IndexQueryResults const& other) const {
    const StringPool *spool = spool_;
    if (spool == nullptr) {
        spool = other.spool_;
    }
    IndexQueryResults result(postinglist_ ^ other.postinglist_, spool);
    return result;
}

IndexQueryResults IndexQueryResults::join(IndexQueryResults const& other) const {
    const StringPool *spool = spool_;
    if (spool == nullptr) {
        spool = other.spool_;
    }
    IndexQueryResults result(postinglist_ | other.postinglist_, spool);
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


//               //
//  IncludeTags  //
//               //

IndexQueryResults IncludeIfAllTagsMatch::query(IndexBase const& index) const {
    IndexQueryResults results = index.metric_query(metric_);
    for(auto const& tv: pairs_) {
        auto res = index.tagvalue_query(tv);
        results = results.intersection(res);
    }
    return results.filter(metric_).filter(pairs_);
}

//                    //
//  IncludeMany2Many  //
//                    //

IncludeMany2Many::IncludeMany2Many(std::string mname, std::map<std::string, std::vector<std::string>> const& map)
    : IndexQueryNodeBase(node_name_)
    , metric_(mname.data(), mname.data() + mname.size())
    , tags_(map)
{
}

IndexQueryResults IncludeMany2Many::query(IndexBase const& index) const {
    std::vector<TagValuePair> tgv;
    IndexQueryResults final_res;
    bool first = true;
    for (auto kv: tags_) {
        if (kv.second.size() > 0) {
            std::stringstream pair;
            pair << kv.first << "=" << kv.second[0];
            TagValuePair tagval(pair.str());
            auto results = index.tagvalue_query(tagval);
            tgv.push_back(tagval);
            for (size_t ix = 1; ix < kv.second.size(); ix++) {
                std::stringstream ixpair;
                ixpair << kv.first << "=" << kv.second[ix];
                TagValuePair ixtagval(ixpair.str());
                tgv.push_back(ixtagval);
                auto res = index.tagvalue_query(ixtagval);
                results = results.join(res).unique();
            }
            if (first) {
                final_res = std::move(results);
                first = false;
            } else {
                final_res = final_res.intersection(results);
            }
        }
    }
    auto allmetric = index.metric_query(metric_);
    if (tgv.empty()) {
        // Select by metric only
        return allmetric.filter(metric_);
    }
    final_res = final_res.intersection(allmetric);
    return final_res.filter(metric_).filter(tgv);
}

//                   //
//  IncludeIfHasTag  //
//                   //

IndexQueryResults IncludeIfHasTag::query(IndexBase const& index) const {
    // Query available tag=value pairs first
    std::map<std::string, std::vector<std::string>> pairs;
    for (auto tag: tagnames_) {
        std::vector<std::string> values;
        auto res = index.list_tag_values(tostrt(metric_), tostrt(tag));
        for (auto val: res) {
            values.push_back(fromstrt(val));
        }
        pairs[tag] = values;
    }
    IncludeMany2Many subquery(metric_, pairs);
    return subquery.query(index);
}


//               //
//  ExcludeTags  //
//               //


IndexQueryResults ExcludeTags::query(IndexBase const& index) const {
    IndexQueryResults results = index.metric_query(metric_);
    for(auto const& tv: pairs_) {
        auto res = index.tagvalue_query(tv);
        results = results.difference(res);
    }
    return results.filter(metric_);
}


//              //
//  JoinByTags  //
//              //

IndexQueryResults JoinByTags::query(IndexBase const& index) const {
    IndexQueryResults results;
    for(auto const& m: metrics_) {
        auto res = index.metric_query(m);
        results = results.join(res);
    }
    for(auto const& tv: pairs_) {
        auto res = index.tagvalue_query(tv);
        results.difference(res);
    }
    return results.filter(metrics_).filter(pairs_);
}


//                      //
//  SeriesNameTopology  //
//                      //


SeriesNameTopology::SeriesNameTopology()
    : index_(StringTools::create_l3_table(1000))
{
}

void SeriesNameTopology::add_name(StringT name) {
    StringT metric = skip_metric_name(name.first, name.first + name.second);
    StringT tags = std::make_pair(name.first + metric.second, name.second - metric.second);
    auto it = index_.find(metric);
    if (it == index_.end()) {
        StringTools::L2TableT tagtable = StringTools::create_l2_table(1024);
        index_[metric] = std::move(tagtable);
        it = index_.find(metric);
    }
    // Iterate through tags
    const char* p = tags.first;
    const char* end = p + tags.second;
    p = skip_space(p, end);
    if (p == end) {
        return;
    }
    // Check tags
    bool error = false;
    while (!error && p < end) {
        const char* tag_start = p;
        const char* tag_end = skip_tag(tag_start, end, &error);
        auto tagstr = std::make_pair(tag_start, tag_end - tag_start);
        StringT tag;
        StringT val;
        if (!split_pair(tagstr, &tag, &val)) {
            error = true;
        }
        StringTools::L2TableT& tagtable = it->second;
        auto tagit = tagtable.find(tag);
        if (tagit == tagtable.end()) {
            auto valtab = StringTools::create_set(1024);
            tagtable[tag] = std::move(valtab);
            tagit = tagtable.find(tag);
        }
        StringTools::SetT& valueset = tagit->second;
        valueset.insert(val);
        // next
        p = skip_space(tag_end, end);
    }
}

std::vector<StringT> SeriesNameTopology::list_metric_names() const {
    std::vector<StringT> res;
    std::transform(index_.begin(), index_.end(), std::back_inserter(res),
                   [](std::pair<StringT, StringTools::L2TableT> const& v) {
                        return v.first;
                   });
    return res;
}

std::vector<StringT> SeriesNameTopology::list_tags(StringT metric) const {
    std::vector<StringT> res;
    auto it = index_.find(metric);
    if (it == index_.end()) {
        return res;
    }
    std::transform(it->second.begin(), it->second.end(), std::back_inserter(res),
                   [](std::pair<StringT, StringTools::SetT> const& v) {
                        return v.first;
                   });
    return res;
}

std::vector<StringT> SeriesNameTopology::list_tag_values(StringT metric, StringT tag) const {
    std::vector<StringT> res;
    auto it = index_.find(metric);
    if (it == index_.end()) {
        return res;
    }
    auto vit = it->second.find(tag);
    if (vit == it->second.end()) {
        return res;
    }
    const auto& set = vit->second;
    std::copy(set.begin(), set.end(), std::back_inserter(res));
    return res;
}

//         //
//  Index  //
//         //

Index::Index()
    : table_(StringTools::create_table(100000))
    , metrics_names_(1024)
    , tagvalue_pairs_(1024)
{
}

SeriesNameTopology const& Index::get_topology() const {
    return topology_;
}

size_t Index::cardinality() const {
    return table_.size();
}

size_t Index::memory_use() const {
    // TODO: use counting allocator for table_ to provide memory stats
    size_t sm = metrics_names_.get_size_in_bytes();
    size_t st = tagvalue_pairs_.get_size_in_bytes();
    size_t sp = pool_.mem_used();
    return sm + st + sp;
}

size_t Index::index_memory_use() const {
    // TODO: use counting allocator for table_ to provide memory stats
    size_t sm = metrics_names_.get_size_in_bytes();
    size_t st = tagvalue_pairs_.get_size_in_bytes();
    return sm + st;
}

size_t Index::pool_memory_use() const {
    return pool_.mem_used();
}

std::tuple<aku_Status, StringT> Index::append(const char* begin, const char* end) {
    static StringT EMPTY_STRING = std::make_pair(nullptr, 0);
    // Parse string value and sort tags alphabetically
    const char* tags_begin;
    const char* tags_end;
    char buffer[0x1000];
    auto status = SeriesParser::to_canonical_form(begin, end, buffer, buffer + 0x1000, &tags_begin, &tags_end);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, EMPTY_STRING);
    }
    // Check if name is already been added
    auto name = std::make_pair(static_cast<const char*>(buffer), tags_end - buffer);
    if (table_.count(name) == 0) {
        // insert value
        auto id = pool_.add(buffer, tags_end);
        if (id == 0) {
            return std::make_tuple(AKU_EBAD_DATA, EMPTY_STRING);
        }
        write_tags(tags_begin, tags_end, &tagvalue_pairs_, id);
        name = pool_.str(id);  // name now have the same lifetime as pool
        table_[name] = id;
        auto mname = skip_metric_name(buffer, tags_begin);
        if (mname.second == 0) {
            return std::make_tuple(AKU_EBAD_DATA, EMPTY_STRING);
        }
        auto mhash = StringTools::hash(mname);
        metrics_names_.add(mhash, id);
        // update topology
        topology_.add_name(name);
        return std::make_tuple(AKU_SUCCESS, name);
    }
    auto it = table_.find(name);
    return std::make_tuple(AKU_SUCCESS, it->first);
}

IndexQueryResults Index::tagvalue_query(const TagValuePair &value) const {
    auto hash = StringTools::hash(value.get_value());
    auto post = tagvalue_pairs_.extract(hash);
    return IndexQueryResults(std::move(post), &pool_);
}

IndexQueryResults Index::metric_query(const MetricName &value) const {
    auto hash = StringTools::hash(value.get_value());
    auto post = metrics_names_.extract(hash);
    return IndexQueryResults(std::move(post), &pool_);
}

std::vector<StringT> Index::list_metric_names() const {
    return topology_.list_metric_names();
}

std::vector<StringT> Index::list_tags(StringT metric) const {
    return topology_.list_tags(metric);
}

std::vector<StringT> Index::list_tag_values(StringT metric, StringT tag) const {
    return topology_.list_tag_values(metric, tag);
}

}  // namespace
