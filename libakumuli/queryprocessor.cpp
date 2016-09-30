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



//  GroupByTag  //
GroupByTag::GroupByTag(const SeriesMatcher& matcher, std::string metric, std::vector<std::string> const& tags)
    : matcher_(matcher)
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

std::unordered_map<aku_ParamId, aku_ParamId> GroupByTag::get_mapping() const {
    return ids_;
}

void GroupByTag::refresh_() {
    auto results = matcher_.regex_match(regex_.c_str(), &offset_, &prev_size_);
    auto filter = StringTools::create_set(tags_.size());
    for (const auto& tag: tags_) {
        filter.insert(std::make_pair(tag.data(), tag.size()));
    }
    char buffer[AKU_LIMITS_MAX_SNAME];
    for (auto item: results) {
        aku_Status status;
        SeriesParser::StringT result, stritem;
        stritem = std::make_pair(std::get<0>(item), std::get<1>(item));
        std::tie(status, result) = SeriesParser::filter_tags(stritem, filter, buffer);
        if (status == AKU_SUCCESS) {
            if (snames_.count(result) == 0) {
                // put result to local stringpool and ids list
                auto localid = local_matcher_.add(result.first, result.first + result.second);
                auto str = local_matcher_.id2str(localid);
                snames_.insert(str);
                ids_[std::get<2>(item)] = localid;
            } else {
                // local name already created
                auto localid = local_matcher_.match(result.first, result.first + result.second);
                if (localid == 0ul) {
                    AKU_PANIC("inconsistent matcher state");
                }
                ids_[std::get<2>(item)] = localid;
            }
        }
    }
}

bool GroupByTag::apply(aku_Sample* sample) {
    if (matcher_.pool.size() != prev_size_) {
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

static QueryRange make_range(aku_Timestamp begin, aku_Timestamp end, QueryRange::QueryRangeType type, OrderBy orderby) {
    return {std::min(begin, end), std::max(begin, end), begin < end ? AKU_CURSOR_DIR_FORWARD : AKU_CURSOR_DIR_BACKWARD, type, orderby};
}

ScanQueryProcessor::ScanQueryProcessor(std::vector<std::shared_ptr<Node>> nodes,
                                       std::string metric,
                                       aku_Timestamp begin,
                                       aku_Timestamp end,
                                       QueryRange::QueryRangeType type,
                                       std::shared_ptr<IQueryFilter> filter,
                                       GroupByTime groupby,
                                       std::shared_ptr<GroupByTag> groupbytag,
                                       OrderBy orderby)
    : range_(make_range(begin, end, type, orderby))
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

    if (range_.is_backward() && range_.type == QueryRange::CONTINUOUS) {
        NodeException err("invalid range field");
        BOOST_THROW_EXCEPTION(err);
    }
}

bool ScanQueryProcessor::get_groupby_mapping(std::unordered_map<aku_ParamId, aku_ParamId>* ids) {
    if (groupby_tag_) {
        *ids = groupby_tag_->get_mapping();
        return true;
    }
    return false;
}

IQueryFilter& ScanQueryProcessor::filter() {
    return *filter_;
}

std::shared_ptr<SeriesMatcher> ScanQueryProcessor::matcher() {
    if (groupby_tag_) {
        return std::shared_ptr<SeriesMatcher>(groupby_tag_, &groupby_tag_->local_matcher_);
    }
    return std::shared_ptr<SeriesMatcher>();
}

bool ScanQueryProcessor::start() {
    return true;
}

bool ScanQueryProcessor::put(const aku_Sample &sample) {
    if (AKU_UNLIKELY(sample.payload.type == aku_PData::EMPTY)) {
        // shourtcut for empty samples
        return last_node_->put(sample);
    }
    /* NOTE: group_by processing is done on column-store level now,
     *       because of that groupby_tag_ is not used here to transform
     *       each sample.
     */
    return groupby_.put(sample, *root_node_);
}

void ScanQueryProcessor::stop() {
    root_node_->complete();
}

void ScanQueryProcessor::set_error(aku_Status error) {
    root_node_->set_error(error);
}

QueryRange ScanQueryProcessor::range() const {
    return range_;
}

MetadataQueryProcessor::MetadataQueryProcessor(std::shared_ptr<IQueryFilter> flt, std::shared_ptr<Node> node)
    : filter_(flt)
    , root_(node)
{
}

bool MetadataQueryProcessor::get_groupby_mapping(std::unordered_map<aku_ParamId, aku_ParamId>* ids) {
    return false;
}

std::shared_ptr<SeriesMatcher> MetadataQueryProcessor::matcher() {
    return std::shared_ptr<SeriesMatcher>();
}

QueryRange MetadataQueryProcessor::range() const {
    return QueryRange{AKU_MAX_TIMESTAMP, AKU_MAX_TIMESTAMP, QueryRange::INSTANT};
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
            root_->complete();
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

std::shared_ptr<QP::IStreamProcessor> Builder::build_query_processor(const char* query,
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
            groupbytag.reset(new GroupByTag(matcher, metric, tags));
        }

        // Order-by statment
        auto orderby = parse_orderby(ptree, logger);

        // Read limit/offset
        auto limoff = parse_limit_offset(ptree, logger);

        // Read select statment
        auto select = parse_select_stmt(ptree, logger);

        // Read sampling method
        auto sampling_params = ptree.get_child_optional("sample");

        // Read where clause
        auto filter = parse_where_clause(ptree, metric, "in", matcher, logger);

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
            return std::make_shared<ScanQueryProcessor>(allnodes, metric, ts_begin, ts_end,
                                                        QueryRange::INSTANT,  // TODO: parse from query
                                                        filter,
                                                        groupbytime,
                                                        std::move(groupbytag),
                                                        orderby);
        }
        return std::make_shared<MetadataQueryProcessor>(filter, next);

    } catch(std::exception const& e) {
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }
}

}} // namespace
