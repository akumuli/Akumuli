#pragma once
#include "akumuli_def.h"
#include <string>

namespace Akumuli {
namespace QP {

enum class Aggregation {
    MIN,
    MAX,
    COUNT,
    AVG,
    SUM,
};

struct Query {
    std::string filter;
    aku_Timestamp begin;
    aku_Timestamp end;
    std::string error;
    Aggregation agg;

    aku_Timestamp get_begin() const;
    aku_Timestamp get_end() const;
    std::string   get_filter() const;

    std::string get_error_message() const;
};

class QueryParser {
public:
    static std::tuple<aku_Status, Query> parse(const char* begin, const char* end);
    static std::tuple<aku_Status, Query> parse(std::string const& str);
};

}}  // namespace
