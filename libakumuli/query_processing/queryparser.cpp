#include "queryparser.h"
#include "log_iface.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>


namespace Akumuli {
namespace QP {

static boost::property_tree::ptree from_json(std::string json) {
    //! C-string to streambuf adapter
    struct MemStreambuf : std::streambuf {
        MemStreambuf(const char* buf) {
            char* p = const_cast<char*>(buf);
            setg(p, p, p+strlen(p));
        }
    };

    boost::property_tree::ptree ptree;
    MemStreambuf strbuf(json.data());
    std::istream stream(&strbuf);
    boost::property_tree::json_parser::read_json(stream, ptree);
    return ptree;
}

std::string Query::get_error_message() const {
    return error;
}

aku_Timestamp Query::get_begin() const {
    return begin;
}

aku_Timestamp Query::get_end() const {
    return end;
}

std::string   Query::get_filter() const {
    return filter;
}

std::tuple<aku_Status, Query> QueryParser::parse(const char* begin, const char* end) {
    std::string text(begin, end);
    return parse(text);
}

std::tuple<aku_Status, Query> QueryParser::parse(std::string const& text) {
    aku_Status status = AKU_SUCCESS;
    Query query;
    boost::property_tree::ptree json = from_json(text);
    try {
        // Parse query
        query.begin = json.get<aku_Timestamp>("begin");
        query.end = json.get<aku_Timestamp>("end");
        query.filter = json.get<std::string>("filter");
    } catch (const boost::property_tree::ptree_error& err) {
        query.error = err.what();
        status = AKU_EQUERY_PARSING_ERROR;
    }
    return std::make_tuple(status, query);
}

}}  // namespace
