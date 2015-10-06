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

namespace Akumuli {
namespace QP {

//                                   //
//         Factory methods           //
//                                   //

static AnomalyDetector::FcastMethod parse_anomaly_detector_type(boost::property_tree::ptree const& ptree) {
    bool approx = ptree.get<bool>("approx");
    std::string name = ptree.get<std::string>("method");
    AnomalyDetector::FcastMethod method;
    if (name == "ewma" || name == "exp-smoothing") {
        method = approx ? AnomalyDetector::EWMA_SKETCH : AnomalyDetector::EWMA;
    } else if (name == "sma" || name == "simple-moving-average") {
        method = approx ? AnomalyDetector::SMA_SKETCH : AnomalyDetector::SMA;
    } else if (name == "double-exp-smoothing") {
        method = approx ? AnomalyDetector::DOUBLE_EXP_SMOOTHING_SKETCH : AnomalyDetector::DOUBLE_EXP_SMOOTHING;
    } else if (name == "holt-winters") {
        method = approx ? AnomalyDetector::HOLT_WINTERS_SKETCH : AnomalyDetector::HOLT_WINTERS;
    } else {
        QueryParserError err("Unknown forecasting method");
        BOOST_THROW_EXCEPTION(err);
    }
    return method;
}

void validate_sketch_params(boost::property_tree::ptree const& ptree) {
    uint32_t bits = ptree.get<uint32_t>("bits", 8);
    uint32_t hashes = ptree.get<uint32_t>("hashes", 1);
    // bits should be in range
    if (bits < 8 || bits > 16) {
        QueryParserError err("Anomaly detector parameter `bits` out of range");
        BOOST_THROW_EXCEPTION(err);
    }
    // hashes should be in range and odd
    if (hashes % 2 == 0) {
        QueryParserError err("Anomaly detector parameter `hashes` should be odd");
        BOOST_THROW_EXCEPTION(err);
    }
    if (hashes == 0 || hashes > 9) {
        QueryParserError err("Anomaly detector parameter `hashes` out of range");
        BOOST_THROW_EXCEPTION(err);
    }
}

void validate_all_params(std::vector<std::string> required, boost::property_tree::ptree const& ptree) {
    for (auto name: required) {
        auto o = ptree.get_optional<std::string>(name);
        if (!o) {
            std::string err_msg = "Parameter " + name + " should be set";
            QueryParserError err(err_msg.c_str());
            BOOST_THROW_EXCEPTION(err);
        }
    }
}

static void validate_anomaly_detector_params(boost::property_tree::ptree const& ptree) {
    auto type = parse_anomaly_detector_type(ptree);
    switch(type) {
    case AnomalyDetector::SMA_SKETCH:
        validate_sketch_params(ptree);
    case AnomalyDetector::SMA:
        validate_all_params({"period"}, ptree);
        break;

    case AnomalyDetector::EWMA_SKETCH:
        validate_sketch_params(ptree);
    case AnomalyDetector::EWMA:
        validate_all_params({"alpha"}, ptree);
        break;

    case AnomalyDetector::DOUBLE_EXP_SMOOTHING_SKETCH:
        validate_sketch_params(ptree);
    case AnomalyDetector::DOUBLE_EXP_SMOOTHING:
        validate_all_params({"alpha", "gamma"}, ptree);
        break;

    case AnomalyDetector::HOLT_WINTERS_SKETCH:
        validate_sketch_params(ptree);
    case AnomalyDetector::HOLT_WINTERS:
        validate_all_params({"alpha", "beta", "gamma", "period"}, ptree);
        break;
    }
}

static void validate_coef(double value, double range_begin, double range_end, const char* err_msg) {
    if (value >= range_begin && value <= range_end) {
        return;
    }
    QueryParserError err(err_msg);
    BOOST_THROW_EXCEPTION(err);
}

static std::shared_ptr<Node> make_sampler(boost::property_tree::ptree const& ptree,
                                          std::shared_ptr<Node> next,
                                          aku_logger_cb_t logger)
{
    try {
        std::string name;
        name = ptree.get<std::string>("name");
        if (name == "reservoir") {
            std::string size = ptree.get<std::string>("size");
            uint32_t nsize = boost::lexical_cast<uint32_t>(size);
            return std::make_shared<RandomSamplingNode>(nsize, next);
        } else if (name == "PAA") {
            return std::make_shared<MeanPAA>(next);
        } else if (name == "PAA-median") {
            return std::make_shared<MedianPAA>(next);
        } else if (name == "frequent-items") {
            std::string serror = ptree.get<std::string>("error");
            std::string sportion = ptree.get<std::string>("portion");
            double error = boost::lexical_cast<double>(serror);
            double portion = boost::lexical_cast<double>(sportion);
            return std::make_shared<SpaceSaver<false>>(error, portion, next);
        } else if (name == "heavy-hitters") {
            std::string serror = ptree.get<std::string>("error");
            std::string sportion = ptree.get<std::string>("portion");
            double error = boost::lexical_cast<double>(serror);
            double portion = boost::lexical_cast<double>(sportion);
            return std::make_shared<SpaceSaver<true>>(error, portion, next);
        } else if (name == "anomaly-detector") {
            validate_anomaly_detector_params(ptree);
            double threshold = ptree.get<double>("threshold");
            uint32_t bits = ptree.get<uint32_t>("bits", 10u);
            uint32_t hashes = ptree.get<uint32_t>("hashes", 3u);
            AnomalyDetector::FcastMethod method = parse_anomaly_detector_type(ptree);
            double alpha = ptree.get<double>("alpha", 0.0);
            double beta = ptree.get<double>("beta", 0.0);
            double gamma = ptree.get<double>("gamma", 0.0);
            int period = ptree.get<int>("period", 0);
            validate_coef(alpha, 0.0, 1.0, "`alpha` should be in [0, 1] range");
            validate_coef(beta,  0.0, 1.0, "`beta` should be in [0, 1] range");
            validate_coef(gamma, 0.0, 1.0, "`gamma` should be in [0, 1] range");
            return std::make_shared<AnomalyDetector>(hashes, bits, threshold, alpha, beta, gamma, period, method, next);
        } else if (name == "SAX") {
            int alphabet_size = ptree.get<int>("alphabet_size");
            int window_width  = ptree.get<int>("window_width");
            bool disable_val  = ptree.get<bool>("no_value", true);
            validate_coef(alphabet_size, 1.0, 20.0, "`alphabet_size` should be in [1, 20] range");
            validate_coef(window_width, 4.0, 100.0, "`window_width` should be in [4, 100] range");
            return std::make_shared<SAXNode>(alphabet_size, window_width, disable_val, next);
        }
        // only this one is implemented
        NodeException except(Node::RandomSampler, "invalid sampler description, unknown algorithm");
        BOOST_THROW_EXCEPTION(except);
    } catch (const boost::property_tree::ptree_error&) {
        NodeException except(Node::RandomSampler, "invalid sampler description");
        BOOST_THROW_EXCEPTION(except);
    } catch (const boost::bad_lexical_cast&) {
        NodeException except(Node::RandomSampler, "invalid sampler description, valid integer expected");
        BOOST_THROW_EXCEPTION(except);
    }
}

static std::shared_ptr<Node> make_filter_by_id_list(std::vector<aku_ParamId> ids,
                                                    std::shared_ptr<Node> next,
                                                    aku_logger_cb_t logger)
{
    struct Matcher {
        std::unordered_set<aku_ParamId> idset;

        bool operator () (aku_ParamId id) {
            return idset.count(id) > 0;
        }
    };
    typedef FilterByIdNode<Matcher> NodeT;
    std::unordered_set<aku_ParamId> idset(ids.begin(), ids.end());
    Matcher fn = { idset };
    std::stringstream logfmt;
    logfmt << "Creating id-list filter node (" << ids.size() << " ids in a list)";
    (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    return std::make_shared<NodeT>(fn, next);
}

static std::shared_ptr<Node> make_filter_out_by_id_list(std::vector<aku_ParamId> ids,
                                                        std::shared_ptr<Node> next,
                                                        aku_logger_cb_t logger)
{
    struct Matcher {
        std::unordered_set<aku_ParamId> idset;

        bool operator () (aku_ParamId id) {
            return idset.count(id) == 0;
        }
    };
    typedef FilterByIdNode<Matcher> NodeT;
    std::unordered_set<aku_ParamId> idset(ids.begin(), ids.end());
    Matcher fn = { idset };
    std::stringstream logfmt;
    logfmt << "Creating id-list filter out node (" << ids.size() << " ids in a list)";
    (*logger)(AKU_LOG_TRACE, logfmt.str().c_str());
    return std::make_shared<NodeT>(fn, next);
}


GroupByStatement::GroupByStatement()
    : step_(0)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByStatement::GroupByStatement(aku_Timestamp step)
    : step_(step)
    , first_hit_(true)
    , lowerbound_(AKU_MIN_TIMESTAMP)
    , upperbound_(AKU_MIN_TIMESTAMP)
{
}

GroupByStatement::GroupByStatement(const GroupByStatement& other)
    : step_(other.step_)
    , first_hit_(other.first_hit_)
    , lowerbound_(other.lowerbound_)
    , upperbound_(other.upperbound_)
{
}

GroupByStatement& GroupByStatement::operator = (const GroupByStatement& other) {
    step_ = other.step_;
    first_hit_ = other.first_hit_;
    lowerbound_ = other.lowerbound_;
    upperbound_ = other.upperbound_;
    return *this;
}

bool GroupByStatement::put(aku_Sample const& sample, Node& next) {
    if (step_) {
        aku_Timestamp ts = sample.timestamp;
        if (AKU_UNLIKELY(first_hit_ == true)) {
            first_hit_ = false;
            aku_Timestamp aligned = ts / step_ * step_;
            lowerbound_ = aligned;
            upperbound_ = aligned + step_;
        }
        if (ts >= upperbound_) {
            // Forward direction
            aku_Sample empty = EMPTY_SAMPLE;
            empty.timestamp = upperbound_;
            if (!next.put(empty)) {
                return false;
            }
            lowerbound_ += step_;
            upperbound_ += step_;
        } else if (ts < lowerbound_) {
            // Backward direction
            aku_Sample empty = EMPTY_SAMPLE;
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

ScanQueryProcessor::ScanQueryProcessor(std::shared_ptr<Node> root,
                                       std::vector<std::string> metrics,
                                       aku_Timestamp begin,
                                       aku_Timestamp end,
                                       GroupByStatement groupby)
    : lowerbound_(std::min(begin, end))
    , upperbound_(std::max(begin, end))
    , direction_(begin > end ? AKU_CURSOR_DIR_BACKWARD : AKU_CURSOR_DIR_FORWARD)
    , metrics_(metrics)
    , namesofinterest_(StringTools::create_table(0x1000))
    , groupby_(groupby)
    , root_node_(root)
{
}

bool ScanQueryProcessor::start() {
    return true;
}

bool ScanQueryProcessor::put(const aku_Sample &sample) {
    return groupby_.put(sample, *root_node_);
}

void ScanQueryProcessor::stop() {
    root_node_->complete();
}

void ScanQueryProcessor::set_error(aku_Status error) {
    std::cerr << "ScanQueryProcessor->error" << std::endl;
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

MetadataQueryProcessor::MetadataQueryProcessor(std::vector<aku_ParamId> ids, std::shared_ptr<Node> node)
    : ids_(ids)
    , root_(node)
{
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

bool MetadataQueryProcessor::start() {
    for (aku_ParamId id: ids_) {
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

static QP::GroupByStatement parse_groupby(boost::property_tree::ptree const& ptree,
                                          aku_logger_cb_t logger) {
    aku_Timestamp duration = 0u;
    auto groupby = ptree.get_child_optional("group-by");
    if (groupby) {
        for(auto child: *groupby) {
            if (child.first == "time") {
                std::string str = child.second.get_value<std::string>();
                duration = DateTimeUtil::parse_duration(str.c_str(), str.size());
            }
        }
    }
    return QP::GroupByStatement(duration);
}

static std::vector<std::string> parse_metric(boost::property_tree::ptree const& ptree,
                                             aku_logger_cb_t logger) {
    std::vector<std::string> metrics;
    auto metric = ptree.get_child_optional("metric");
    if (metric) {
        auto single = metric->get_value<std::string>();
        if (single.empty()) {
            for(auto child: *metric) {
                auto metric_name = child.second.get_value<std::string>();
                metrics.push_back(metric_name);
            }
        } else {
            metrics.push_back(single);
        }
    }
    return metrics;
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


static aku_ParamId extract_id_from_pool(StringPool::StringT res) {
    // Series name in string pool should be followed by \0 character and 64-bit series id.
    auto p = res.first + res.second;
    assert(p[0] == '\0');
    p += 1;  // zero terminator + sizeof(uint64_t)
    return *reinterpret_cast<uint64_t const*>(p);
}

static std::vector<aku_ParamId> parse_where_clause(boost::property_tree::ptree const& ptree,
                                                   std::string metric,
                                                   std::string pred,
                                                   StringPool const& pool,
                                                   aku_logger_cb_t logger)
{
    std::vector<aku_ParamId> ids;
    bool not_set = false;
    auto where = ptree.get_child_optional("where");
    if (where) {
        for (auto child: *where) {
            auto predicate = child.second;
            auto items = predicate.get_child_optional(pred);
            if (items) {
                for (auto item: *items) {
                    std::string tag = item.first;
                    auto idslist = item.second;
                    // Read idlist
                    for (auto idnode: idslist) {
                        std::string value = idnode.second.get_value<std::string>();
                        std::stringstream series_regexp;
                        series_regexp << "(" << metric << R"((?:\s\w+=\w+)*\s)"
                                      << tag << "=" << value << R"((?:\s\w+=\w+)*))";
                        std::string regex = series_regexp.str();
                        auto results = pool.regex_match(regex.c_str());
                        for(auto res: results) {
                            aku_ParamId id = extract_id_from_pool(res);
                            ids.push_back(id);
                        }
                    }
                }
            } else {
                not_set = true;
            }
        }
    } else {
        not_set = true;
    }
    if (not_set) {
        if (pred == "in") {
            // there is no "in" predicate so we need to include all
            // series from this metric
            std::stringstream series_regexp;
            series_regexp << "" << metric << R"((\s\w+=\w+)*)";
            std::string regex = series_regexp.str();
            auto results = pool.regex_match(regex.c_str());
            for(auto res: results) {
                aku_ParamId id = extract_id_from_pool(res);
                ids.push_back(id);
            }
        }
    }
    return ids;
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
        // Read groupby statement
        auto groupby = parse_groupby(ptree, logger);

        // Read metric(s) name
        auto metrics = parse_metric(ptree, logger);

        // Read select statment
        auto select = parse_select_stmt(ptree, logger);

        // Read sampling method
        auto sampling_params = ptree.get_child_optional("sample");

        // Read where clause
        std::vector<aku_ParamId> ids_included;
        std::vector<aku_ParamId> ids_excluded;

        for(auto metric: metrics) {

            auto in = parse_where_clause(ptree, metric, "in", matcher.pool, logger);
            std::copy(in.begin(), in.end(), std::back_inserter(ids_included));

            auto notin = parse_where_clause(ptree, metric, "not_in", matcher.pool, logger);
            std::copy(notin.begin(), notin.end(), std::back_inserter(ids_excluded));
        }

        if (sampling_params && select) {
            (*logger)(AKU_LOG_ERROR, "Can't combine select and sample statements together");
            auto rte = std::runtime_error("`sample` and `select` can't be used together");
            BOOST_THROW_EXCEPTION(rte);
        }

        // Build topology
        std::shared_ptr<Node> next = terminal;
        if (!select) {
            // Read timestamps
            auto ts_begin = parse_range_timestamp(ptree, "from", logger);
            auto ts_end = parse_range_timestamp(ptree, "to", logger);

            if (sampling_params) {
                for (auto i = sampling_params->rbegin(); i != sampling_params->rend(); i++) {
                        next = make_sampler(i->second, next, logger);
                }
            }
            if (!ids_included.empty()) {
                next = make_filter_by_id_list(ids_included, next, logger);
            }
            if (!ids_excluded.empty()) {
                next = make_filter_out_by_id_list(ids_excluded, next, logger);
            }
            // Build query processor
            return std::make_shared<ScanQueryProcessor>(next, metrics, ts_begin, ts_end, groupby);
        }

        if (ids_included.empty() && metrics.empty()) {
            // list all
            for (auto val: matcher.table) {
                auto id = val.second;
                ids_included.push_back(id);
            }
        }
        if (!ids_excluded.empty()) {
            std::sort(ids_included.begin(), ids_included.end());
            std::sort(ids_excluded.begin(), ids_excluded.end());
            std::vector<aku_ParamId> tmp;
            std::set_difference(ids_included.begin(), ids_included.end(),
                                ids_excluded.begin(), ids_excluded.end(),
                                std::back_inserter(tmp));
            std::swap(tmp, ids_included);
        }
        return std::make_shared<MetadataQueryProcessor>(ids_included, next);

    } catch(std::exception const& e) {
        (*logger)(AKU_LOG_ERROR, e.what());
        throw QueryParserError(e.what());
    }
}

}} // namespace
