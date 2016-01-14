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
 */

#include "queryprocessor.h"
#include "util.h"
#include "datetime.h"
#include "anomalydetector.h"
#include "saxencoder.h"

#include <random>
#include <algorithm>
#include <unordered_set>
#include <set>

#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/exception/diagnostic_information.hpp>

// Include query processors
#include "query_processing/anomaly.h"
#include "query_processing/filterbyid.h"
#include "query_processing/paa.h"
#include "query_processing/randomsamplingnode.h"
#include "query_processing/sax.h"
#include "query_processing/spacesaver.h"
#include "query_processing/limiter.h"

namespace Akumuli {
namespace QP {

//                                   //
//         Factory methods           //
//                                   //


static std::shared_ptr<Node> make_sampler(boost::property_tree::ptree const& ptree,
                                          std::shared_ptr<Node> next,
                                          aku_logger_cb_t logger)
{
    try {
        std::string name;
        name = ptree.get<std::string>("name");
        return QP::create_node(name, ptree, next);
    } catch (const boost::property_tree::ptree_error&) {
        QueryParserError except("invalid sampler description");
        BOOST_THROW_EXCEPTION(except);
    }
}



struct RegexFilter : IQueryFilter {
    std::string regex_;
    std::unordered_set<aku_ParamId> ids_;
    StringPool const& spool_;
    StringPoolOffset offset_;
    size_t prev_size_;

    RegexFilter(std::string regex, StringPool const& spool)
        : regex_(regex)
        , spool_(spool)
        , offset_{}
        , prev_size_(0ul)
    {
        refresh();
    }

    void refresh() {
        std::vector<StringPool::StringT> results = spool_.regex_match(regex_.c_str(), &offset_, &prev_size_);
        int ix = 0;
        for (StringPool::StringT item: results) {
            auto id = StringTools::extract_id_from_pool(item);
            ids_.insert(id);
            ix++;
        }
    }

    virtual std::vector<aku_ParamId> get_ids() {
        std::vector<aku_ParamId> result;
        std::copy(ids_.begin(), ids_.end(), std::back_inserter(result));
        return result;
    }

    virtual FilterResult apply(aku_ParamId id) {
        // Atomic operation, can be a source of contention
        if (spool_.size() != prev_size_) {
            refresh();
        }
        return ids_.count(id) != 0 ? PROCESS : SKIP_THIS;
    }
};


GroupByTime::GroupByTime()
    : step_(0)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByTime::GroupByTime(aku_Timestamp step)
    : step_(step)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByTime::GroupByTime(const GroupByTime& other)
    : step_(other.step_)
    , first_hit_(other.first_hit_)
    , lowerbound_(other.lowerbound_)
    , upperbound_(other.upperbound_)
{
}

GroupByTime& GroupByTime::operator = (const GroupByTime& other) {
    step_ = other.step_;
    first_hit_ = other.first_hit_;
    lowerbound_ = other.lowerbound_;
    upperbound_ = other.upperbound_;
    return *this;
}

bool GroupByTime::put(aku_Sample const& sample, Node& next) {
    if (step_ && sample.payload.type != aku_PData::EMPTY) {
        aku_Timestamp ts = sample.timestamp;
        if (AKU_UNLIKELY(first_hit_ == true)) {
            first_hit_ = false;
            aku_Timestamp aligned = ts / step_ * step_;
            lowerbound_ = aligned;
            upperbound_ = aligned + step_;
        }
        if (ts >= upperbound_) {
            // Forward direction
            aku_Sample empty = SAMPLING_HI_MARGIN;
            empty.timestamp = upperbound_;
            if (!next.put(empty)) {
                return false;
            }
            lowerbound_ += step_;
            upperbound_ += step_;
        } else if (ts < lowerbound_) {
            // Backward direction
            aku_Sample empty = SAMPLING_LO_MARGIN;
            empty.timestamp = upperbound_;
            if (!next.put(empty)) {
                return false;
            }
            lowerbound_ -= step_;
            upperbound_ -= step_;
        }
    }
    return next.put(sample);
}

bool GroupByTime::empty() const {
    return step_ == 0;
}

//  GroupByTag  //
GroupByTag::GroupByTag(StringPool const* spool, std::string metric, std::vector<std::string> const& tags)
    : spool_(spool)
    , offset_{0}
    , prev_size_(0)
    , tags_(tags)
    , local_matcher_(1ul)
    , snames_(StringTools::create_set(64))
{
    std::sort(tags_.begin(), tags_.end());
    // Build regexp
    std::stringstream series_regexp;
    //cpu(?:\s\w+=\w+)* (?:\s\w+=\w+)*\s hash=\w+ (?:\s\w+=\w+)*
    series_regexp << metric << "(?:\\s\\w+=\\w+)*";
    for (auto tag: tags) {
        series_regexp << "(?:\\s\\w+=\\w+)*\\s" << tag << "=\\w+";
    }
    series_regexp << "(?:\\s\\w+=\\w+)*";
    regex_ = series_regexp.str();
    refresh_();
}

void GroupByTag::refresh_() {
    std::vector<StringPool::StringT> results = spool_->regex_match(regex_.c_str(), &offset_, &prev_size_);
    auto filter = StringTools::create_set(tags_.size());
    for (const auto& tag: tags_) {
        filter.insert(std::make_pair(tag.data(), tag.size()));
    }
    char buffer[AKU_LIMITS_MAX_SNAME];
    for (StringPool::StringT item: results) {
        auto id = StringTools::extract_id_from_pool(item);
        aku_Status status;
        SeriesParser::StringT result;
        std::tie(status, result) = SeriesParser::filter_tags(item, filter, buffer);
        if (status == AKU_SUCCESS) {
            if (snames_.count(result) == 0) {
                // put result to local stringpool and ids list
                auto localid = local_matcher_.add(result.first, result.first + result.second);
                auto str = local_matcher_.id2str(localid);
                snames_.insert(str);
                ids_[id] = localid;
            } else {
                // local name already created
                auto localid = local_matcher_.match(result.first, result.first + result.second);
                if (localid == 0ul) {
                    AKU_PANIC("inconsistent matcher state");
                }
                ids_[id] = localid;
            }
        }
    }
}

bool GroupByTag::apply(aku_Sample* sample) {
    if (spool_->size() != prev_size_) {
        refresh_();
    }
    auto it = ids_.find(sample->paramid);
    if (it != ids_.end()) {
        sample->paramid = it->second;
        return true;
    }
    return false;
}


//  ScanQueryProcessor  //

ScanQueryProcessor::ScanQueryProcessor(std::vector<std::shared_ptr<Node>> nodes,
                                       std::string metric,
                                       aku_Timestamp begin,
                                       aku_Timestamp end,
                                       std::shared_ptr<IQueryFilter> filter,
                                       GroupByTime groupby,
                                       std::unique_ptr<GroupByTag> groupbytag
                                       )
    : lowerbound_(std::min(begin, end))
    , upperbound_(std::max(begin, end))
    , direction_(begin > end ? AKU_CURSOR_DIR_BACKWARD : AKU_CURSOR_DIR_FORWARD)
    , metric_(metric)
    , namesofinterest_(StringTools::create_table(0x1000))
    , groupby_(groupby)
    , filter_(filter)
    , groupby_tag_(std::move(groupbytag))
{
    if (nodes.empty()) {
        AKU_PANIC("`nodes` shouldn't be empty")
    }

    root_node_ = nodes.front();
    last_node_ = nodes.back();

    // validate query processor data
    if (groupby_.empty()) {
        for (auto ptr: nodes) {
            if ((ptr->get_requirements() & Node::GROUP_BY_REQUIRED) != 0) {
                NodeException err("`group_by` required");  // TODO: more detailed error message
                BOOST_THROW_EXCEPTION(err);
            }
        }
    }

    int nnormal = 0;
    for (auto it = nodes.rbegin(); it != nodes.rend(); it++) {
        if (((*it)->get_requirements() & Node::TERMINAL) != 0) {
            if (nnormal != 0) {
                NodeException err("invalid sampling order");  // TODO: more detailed error message
                BOOST_THROW_EXCEPTION(err);
            }
        } else {
            nnormal++;
        }
    }
}

IQueryFilter& ScanQueryProcessor::filter() {
    return *filter_;
}

SeriesMatcher* ScanQueryProcessor::matcher() {
    if (groupby_tag_) {
        return &groupby_tag_->local_matcher_;
    }
    return nullptr;
}

bool ScanQueryProcessor::start() {
    return true;
}

bool ScanQueryProcessor::put(const aku_Sample &sample) {
    if (AKU_UNLIKELY(sample.payload.type == aku_PData::EMPTY)) {
        // shourtcut for empty samples
        return last_node_->put(sample);
    }
    // We're dealing with basic sample here (no extra payload)
    // that comes right from page or sequencer. Because of that
    // we can copy it without slicing.
    auto copy = sample;
    if (groupby_tag_ && !groupby_tag_->apply(&copy)) {
        return true;
    }
    return groupby_.put(copy, *root_node_);
}

void ScanQueryProcessor::stop() {
    root_node_->complete();
}

void ScanQueryProcessor::set_error(aku_Status error) {
    root_node_->set_error(error);
}

aku_Timestamp ScanQueryProcessor::lowerbound() const {
    return lowerbound_;
}

aku_Timestamp ScanQueryProcessor::upperbound() const {
    return upperbound_;
}

int ScanQueryProcessor::direction() const {
    return direction_;
}

MetadataQueryProcessor::MetadataQueryProcessor(std::shared_ptr<IQueryFilter> flt, std::shared_ptr<Node> node)
    : filter_(flt)
    , root_(node)
{
}

SeriesMatcher* MetadataQueryProcessor::matcher() {
    return nullptr;
}

aku_Timestamp MetadataQueryProcessor::lowerbound() const {
    return AKU_MAX_TIMESTAMP;
}

aku_Timestamp MetadataQueryProcessor::upperbound() const {
    return AKU_MAX_TIMESTAMP;
}

int MetadataQueryProcessor::direction() const {
    return AKU_CURSOR_DIR_FORWARD;
}

IQueryFilter& MetadataQueryProcessor::filter() {
    return *filter_;
}

bool MetadataQueryProcessor::start() {
    for (auto id: filter_->get_ids()) {
        aku_Sample s;
        s.paramid = id;
        s.timestamp = 0;
        s.payload.type = aku_PData::PARAMID_BIT;
        s.payload.size = sizeof(aku_Sample);
        if (!root_->put(s)) {
            return false;
        }
    }
    return true;
}

bool MetadataQueryProcessor::put(const aku_Sample &sample) {
    // no-op
    return false;
}

void MetadataQueryProcessor::stop() {
    root_->complete();
}

void MetadataQueryProcessor::set_error(aku_Status error) {
    root_->set_error(error);
}



//                          //
//                          //
//   Build query processor  //
//                          //
//                          //

static boost::optional<std::string> parse_select_stmt(boost::property_tree::ptree const& ptree, aku_logger_cb_t logger) {
    auto select = ptree.get_child_optional("select");
    if (select && select->empty()) {
        // simple select query
        auto str = select->get_value<std::string>("");
        if (str == "names") {
            // the only supported select query for now
            return str;
        }
        (*logger)(AKU_LOG_ERROR, "Invalid `select` query");
        auto rte = std::runtime_error("Invalid `select` query");
        BOOST_THROW_EXCEPTION(rte);
    }
    return boost::optional<std::string>();
}

static std::tuple<QP::GroupByTime, std::vector<std::string>> parse_groupby(boost::property_tree::ptree const& ptree,
                                                                           aku_logger_cb_t logger) {
    std::vector<std::string> tags;
    aku_Timestamp duration = 0u;
    auto groupby = ptree.get_child_optional("group-by");
    if (groupby) {
        for(auto child: *groupby) {
            if (child.first == "time") {
                std::string str = child.second.get_value<std::string>();
                duration = DateTimeUtil::parse_duration(str.c_str(), str.size());
            } else if (child.first == "tag") {
                if (!child.second.empty()) {
                    for (auto tag: child.second) {
                        tags.push_back(tag.second.get_value<std::string>());
                    }
                } else {
                    auto tag = child.second.get_value<std::string>();
                    tags.push_back(tag);
                }
            }
        }
    }
    return std::make_tuple(QP::GroupByTime(duration), tags);
}

static std::pair<uint64_t, uint64_t> parse_limit_offset(boost::property_tree::ptree const& ptree, aku_logger_cb_t logger) {
    uint64_t limit = 0ul, offset = 0ul;
    auto optlimit = ptree.get_child_optional("limit");
    if (optlimit) {
        limit = optlimit->get_value<uint64_t>();
    }
    auto optoffset = ptree.get_child_optional("offset");
    if (optoffset) {
        limit = optoffset->get_value<uint64_t>();
    }
    return std::make_pair(limit, offset);
}

static std::string parse_metric(boost::property_tree::ptree const& ptree,
                                aku_logger_cb_t logger) {
    auto opt = ptree.get_child_optional("metric");
    if (opt) {
        auto single = opt->get_value<std::string>();
        return single;
    }
    auto select = ptree.get_child_optional("select");
    if (!select) {
        QueryParserError error("`metric` not set");
        BOOST_THROW_EXCEPTION(error);
    }
    return "";
}

static aku_Timestamp parse_range_timestamp(boost::property_tree::ptree const& ptree,
                                           std::string const& name,
                                           aku_logger_cb_t logger) {
    auto range = ptree.get_child("range");
    for(auto child: range) {
        if (child.first == name) {
            auto iso_string = child.second.get_value<std::string>();
            auto ts = DateTimeUtil::from_iso_string(iso_string.c_str());
            return ts;
        }
    }
    std::stringstream fmt;
    fmt << "can't find `" << name << "` tag inside the query";
    QueryParserError error(fmt.str().c_str());
    BOOST_THROW_EXCEPTION(error);
}

static std::shared_ptr<RegexFilter> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                       std::string metric,
                                                       std::string pred,
                                                       StringPool const& pool,
                                                       aku_logger_cb_t logger)
{
    std::shared_ptr<RegexFilter> result;
    bool not_set = false;
    auto where = ptree.get_child_optional("where");
    if (where) {
        if (metric.empty()) {
            QueryParserError error("metric is not set");
            BOOST_THROW_EXCEPTION(error);
        }
        for (auto item: *where) {
            bool firstitem = true;
            std::stringstream series_regexp;
            std::string tag = item.first;
            auto idslist = item.second;
            // Read idlist
            for (auto idnode: idslist) {
                std::string value = idnode.second.get_value<std::string>();
                if (firstitem) {
                    firstitem = false;
                    series_regexp << "(?:";
                } else {
                    series_regexp << "|";
                }
                series_regexp << "(" << metric << R"((?:\s\w+=\w+)*\s)"
                              << tag << "=" << value << R"((?:\s\w+=\w+)*))";
            }
            series_regexp << ")";
            std::string regex = series_regexp.str();
            result = std::make_shared<RegexFilter>(regex, pool);
        }
    } else {
        not_set = true;
    }
    if (not_set) {
        // we need to include all series
        if (metric.empty()) {
            metric = "\\w+";
        }
        std::stringstream series_regexp;
        series_regexp << metric << "(?:\\s\\w+=\\w+)+";
        std::string regex = series_regexp.str();
        result = std::make_shared<RegexFilter>(regex, pool);
    }
    return result;
}

static std::string to_json(boost::property_tree::ptree const& ptree, bool pretty_print = true) {
    std::stringstream ss;
    boost::property_tree::write_json(ss, ptree, pretty_print);
    return ss.str();
}

std::shared_ptr<QP::IQueryProcessor> Builder::build_query_processor(const char* query,
                                                                    std::shared_ptr<QP::Node> terminal,
                                                                    const SeriesMatcher &matcher,
                                                                    aku_logger_cb_t logger) {
    namespace pt = boost::property_tree;
    using namespace QP;


    const auto NOSAMPLE = std::make_pair<std::string, size_t>("", 0u);

    //! C-string to streambuf adapter
    struct MemStreambuf : std::streambuf {
        MemStreambuf(const char* buf) {
            char* p = const_cast<char*>(buf);
            setg(p, p, p+strlen(p));
        }
    };

    boost::property_tree::ptree ptree;
    MemStreambuf strbuf(query);
    std::istream stream(&strbuf);
    try {
        pt::json_parser::read_json(stream, ptree);
    } catch (pt::json_parser_error& e) {
        // Error, bad query
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }

    logger(AKU_LOG_INFO, "Parsing query:");
    logger(AKU_LOG_INFO, to_json(ptree, true).c_str());

    try {
        // Read metric name
        auto metric = parse_metric(ptree, logger);

        // Read groupby statement
        std::vector<std::string> tags;
        GroupByTime groupbytime;
        std::tie(groupbytime, tags) = parse_groupby(ptree, logger);
        auto groupbytag = std::unique_ptr<GroupByTag>();
        if (!tags.empty()) {
            groupbytag.reset(new GroupByTag(&matcher.pool, metric, tags));
        }

        // Read limit/offset
        auto limoff = parse_limit_offset(ptree, logger);

        // Read select statment
        auto select = parse_select_stmt(ptree, logger);

        // Read sampling method
        auto sampling_params = ptree.get_child_optional("sample");

        // Read where clause
        auto filter = parse_where_clause(ptree, metric, "in", matcher.pool, logger);

        if (sampling_params && select) {
            (*logger)(AKU_LOG_ERROR, "Can't combine select and sample statements together");
            auto rte = std::runtime_error("`sample` and `select` can't be used together");
            BOOST_THROW_EXCEPTION(rte);
        }

        // Build topology
        std::shared_ptr<Node> next = terminal;
        std::vector<std::shared_ptr<Node>> allnodes = { next };
        if (limoff.first != 0 || limoff.second != 0) {
            // Limiter should work with both metadata and scan queryprocessors.
            next = std::make_shared<QP::Limiter>(limoff.first, limoff.second, terminal);
            allnodes.push_back(next);
        }
        if (!select) {
            // Read timestamps
            auto ts_begin = parse_range_timestamp(ptree, "from", logger);
            auto ts_end = parse_range_timestamp(ptree, "to", logger);

            if (sampling_params) {
                for (auto i = sampling_params->rbegin(); i != sampling_params->rend(); i++) {
                    next = make_sampler(i->second, next, logger);
                    allnodes.push_back(next);
                }
            }
            std::reverse(allnodes.begin(), allnodes.end());
            // Build query processor
            return std::make_shared<ScanQueryProcessor>(allnodes, metric, ts_begin, ts_end, filter, groupbytime, std::move(groupbytag));
        }
        return std::make_shared<MetadataQueryProcessor>(filter, next);

    } catch(std::exception const& e) {
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }
}

}} // namespace
