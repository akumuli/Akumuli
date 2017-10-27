#include "query_results_pooler.h"
#include "logger.h"
#include <cstdio>
#include <thread>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/exception/all.hpp>

namespace Akumuli {

static Logger logger("query_results_pooler");

static boost::property_tree::ptree from_json(std::string json) {
    //! C-string to streambuf adapter
    struct MemStreambuf : std::streambuf {
        MemStreambuf(const char* buf) {
            char* p = const_cast<char*>(buf);
            setg(p, p, p+strlen(p));
        }
    };

    boost::property_tree::ptree ptree;
    MemStreambuf strbuf(json.c_str());
    std::istream stream(&strbuf);
    boost::property_tree::json_parser::read_json(stream, ptree);
    return ptree;
}

struct CSVOutputFormatter : OutputFormatter {

    std::shared_ptr<DbSession> session_;
    const bool iso_timestamps_;

    // TODO: parametrize column separator

    CSVOutputFormatter(std::shared_ptr<DbSession> con, bool iso_timestamps)
        : session_(con)
        , iso_timestamps_(iso_timestamps)
    {
    }

    virtual char* format(char* begin, char* end, const aku_Sample& sample) {
        if(begin >= end) {
            return nullptr;  // not enough space inside the buffer
        }
        int size = end - begin;

        bool newline_required = false;

        int len = 0;

        if (sample.payload.type & aku_PData::PARAMID_BIT) {
            // Series name
            len = session_->param_id_to_series(sample.paramid, begin, size);
            // '\0' character is counted in len
            if (len == 0) { // Error, no such Id
                len = snprintf(begin, size, "id=%lu", sample.paramid);
                if (len < 0 || len == size) {
                    // Not enough space inside the buffer
                    return nullptr;
                }
                len += 1;  // for terminating '\0' character
            } else if (len < 0) {
                // Not enough space
                return nullptr;
            }
            begin += len;
            size  -= len;
            newline_required = true;
        }

        if (sample.payload.type & aku_PData::TIMESTAMP_BIT) {
            if (newline_required) {
                // Add trailing ',' to the end
                if (size < 1) {
                    return nullptr;
                }
                begin[0] = ',';
                begin += 1;
                size  -= 1;
            }

            // Timestamp
            if (size < 0) {
                return nullptr;
            }
            if ((sample.payload.type&aku_PData::CUSTOM_TIMESTAMP) == 0 && iso_timestamps_) {
                len = aku_timestamp_to_string(sample.timestamp, begin, size) - 1;  // -1 is for '\0' character
            } else {
                len = -1;
            }
            if (len == -1) {
                // Invalid or custom timestamp, format as number
                len = snprintf(begin, size, "ts=%lu", sample.timestamp);
                if (len < 0 || len == size) {
                    // Not enough space inside the buffer
                    return nullptr;
                }
            } else if (len < -1) {
                return nullptr;
            }
            begin += len;
            size  -= len;
            newline_required = true;
        }

        // Payload
        if (size < 0) {
            return nullptr;
        }

        if (sample.payload.type & aku_PData::FLOAT_BIT) {
            if (newline_required) {
                // Add trailing ',' to the end
                if (size < 1) {
                    return nullptr;
                }
                begin[0] = ',';
                begin += 1;
                size  -= 1;
            }
            // Floating-point
            len = snprintf(begin, size, "%.17g", sample.payload.float64);
            if (len == size || len < 0) {
                return nullptr;
            }
            begin += len;
            size  -= len;
            newline_required = true;
        }

        if (sample.payload.type & aku_PData::TUPLE_BIT) {
            if (newline_required) {
                // Add trailing ',' to the end
                if (size < 1) {
                    return nullptr;
                }
                begin[0] = ',';
                begin += 1;
                size  -= 1;
            }
            union {
                u64 u;
                double d;
            } bits;
            bits.d = sample.payload.float64;
            int nelements = bits.u >> 58;  // top 6 bits contains number of elements
            double const* tuple = reinterpret_cast<double const*>(sample.payload.data);
            int tup_ix = 0;
            for (int ix = 0; ix < nelements; ix++) {
                if (bits.u & (1 << ix)) {
                    if (tup_ix == 0) {
                        len = snprintf(begin, size, "%.17g", tuple[tup_ix]);
                    } else {
                        len = snprintf(begin, size, ",%.17g", tuple[tup_ix]);
                    }
                    tup_ix++;
                } else {
                    len = snprintf(begin, size, ",");
                }
                if (len == size || len < 0) {
                    return nullptr;
                }
                begin += len;
                size  -= len;
                newline_required = true;
            }
        }

        if (sample.payload.type & aku_PData::SAX_WORD) {
            if (newline_required) {
                // Add trailing ',' to the end
                if (size < 1) {
                    return nullptr;
                }
                begin[0] = ',';
                begin += 1;
                size  -= 1;
            }
            size_t sample_size = std::max(sizeof(aku_Sample), (size_t)sample.payload.size);
            int sax_word_sz = static_cast<int>(sample_size - sizeof(aku_Sample));
            if (size < (sax_word_sz + 3)) {
                return nullptr;
            }
            for(int i = 0; i < sax_word_sz; i++) {
                begin[i] = sample.payload.data[i];
            }
            begin += sax_word_sz;
            size  -= sax_word_sz;
            newline_required = true;
        }

        if (newline_required) {
            if (size < 1) {
                return nullptr;
            }
            begin[0] = '\n';
            begin += 1;
            size  -= 1;
        }

        return begin;
    }
};

//! RESP output implementation
struct RESPOutputFormatter : OutputFormatter {

    std::shared_ptr<DbSession> session_;
    const bool iso_timestamps_;

    RESPOutputFormatter(std::shared_ptr<DbSession> con, bool iso_timestamps)
        : session_(con)
        , iso_timestamps_(iso_timestamps)
    {
    }

    virtual char* format(char* begin, char* end, const aku_Sample& sample) {
        // RESP formatted output: +series name\r\n+timestamp\r\n+value\r\n (for double or $value for blob)

        if(begin >= end) {
            return nullptr;  // not enough space inside the buffer
        }
        int size = end - begin;

        begin[0] = '+';
        begin++;
        size--;

        // sz can't be zero here because of precondition

        int len = 0;

        if (sample.payload.type & aku_PData::PARAMID_BIT) {
            // Series name
            len = session_->param_id_to_series(sample.paramid, begin, size);
            // '\0' character is counted in len
            if (len == 0) { // Error, no such Id
                len = snprintf(begin, size, "id=%lu", sample.paramid);
                if (len < 0 || len == size) {
                    // Not enough space inside the buffer
                    return nullptr;
                }
                len += 1;  // for terminating '\0' character
            } else if (len < 0) {
                // Not enough space
                return nullptr;
            }
            begin += len;
            size  -= len;
            // Add trailing \r\n to the end
            if (size < 2) {
                return nullptr;
            }
            begin[0] = '\r';
            begin[1] = '\n';
            begin += 2;
            size  -= 2;
        }

        if (sample.payload.type & aku_PData::TIMESTAMP_BIT) {
            // Timestamp
            begin[0] = '+';
            begin++;
            size--;
            if (size < 0) {
                return nullptr;
            }
            if ((sample.payload.type&aku_PData::CUSTOM_TIMESTAMP) == 0 && iso_timestamps_) {
                len = aku_timestamp_to_string(sample.timestamp, begin, size) - 1;  // -1 is for '\0' character
            } else {
                len = -1;
            }
            if (len == -1) {
                // Invalid or custom timestamp, format as number
                len = snprintf(begin, size, "ts=%lu", sample.timestamp);
                if (len < 0 || len == size) {
                    // Not enough space inside the buffer
                    return nullptr;
                }
            } else if (len < -1) {
                return nullptr;
            }
            begin += len;
            size  -= len;
            // Add trailing \r\n to the end
            if (size < 2) {
                return nullptr;
            }
            begin[0] = '\r';
            begin[1] = '\n';
            begin += 2;
            size  -= 2;
        }

        // Payload
        if (size < 0) {
            return nullptr;
        }

        if (sample.payload.type & aku_PData::FLOAT_BIT) {
            // Floating-point
            len = snprintf(begin, size, "+%.17g\r\n", sample.payload.float64);
            if (len == size || len < 0) {
                return nullptr;
            }
            begin += len;
            size  -= len;
        }

        if (sample.payload.type & aku_PData::TUPLE_BIT) {
            union {
                u64 u;
                double d;
            } bits;
            bits.d = sample.payload.float64;
            int nelements = bits.u >> 58;  // top 6 bits contains number of elements

            // Output RESP array, start with number of elements
            len = snprintf(begin, size, "*%d\r\n", nelements);
            if (len == size || len < 0) {
                return nullptr;
            }
            begin += len;
            size  -= len;

            // Output array elements
            double const* tuple = reinterpret_cast<double const*>(sample.payload.data);
            int tup_ix = 0;
            for (int ix = 0; ix < nelements; ix++) {
                if (bits.u & (1 << ix)) {
                    len = snprintf(begin, size, "+%.17g\r\n", tuple[tup_ix++]);
                } else {
                    // Empty tuple value encountered. RESP uses bulk string with length equal to -1
                    // to represent Null values.
                    len = snprintf(begin, size, "$-1\r\n");
                }
                if (len == size || len < 0) {
                    return nullptr;
                }
                begin += len;
                size  -= len;
            }
        }

        if (sample.payload.type & aku_PData::SAX_WORD) {
            size_t sample_size = std::max(sizeof(aku_Sample), (size_t)sample.payload.size);
            int sax_word_sz = static_cast<int>(sample_size - sizeof(aku_Sample));
            if (size < (sax_word_sz + 3)) {
                return nullptr;
            }
            for(int i = 0; i < sax_word_sz; i++) {
                begin[i] = sample.payload.data[i];
            }
            begin += sax_word_sz;
            size  -= sax_word_sz;
            begin[0] = '\r';
            begin[1] = '\n';
            begin += 2;
            size  -= 2;
        }
        return begin;
    }
};

QueryResultsPooler::QueryResultsPooler(std::shared_ptr<DbSession> session, int readbufsize, ApiEndpoint endpoint)
    : session_(session)
    , rdbuf_pos_(0)
    , rdbuf_top_(0)
    , endpoint_(endpoint)
{
    try {
        rdbuf_.resize(readbufsize);
    } catch (const std::bad_alloc&) {
        // readbufsize is too large (bad config probably), use default value
        rdbuf_.resize(DEFAULT_RDBUF_SIZE_);
    }
}

void QueryResultsPooler::throw_if_started() const {
    if (cursor_) {
        BOOST_THROW_EXCEPTION(std::runtime_error("allready started"));
    }
}

void QueryResultsPooler::throw_if_not_started() const {
    if (!cursor_) {
        BOOST_THROW_EXCEPTION(std::runtime_error("not started"));
    }
}

void QueryResultsPooler::start() {
    throw_if_started();
    enum Format { RESP, CSV };
    bool use_iso_timestamps = true;
    Format output_format = RESP;
    boost::property_tree::ptree tree;
    try {
        tree = from_json(query_text_);
    } catch (boost::property_tree::json_parser_error const& e) {
        logger.error() << "Bad JSON document received, error: " << e.what();
        // We need to pass invalid document further to generate proper error response
        _init_cursor();
        return;
    }
    auto output = tree.get_child_optional("output");
    if (output) {
        for (auto kv: *output) {
            if (kv.first == "timestamp") {
                std::string ts = kv.second.get_value<std::string>();
                if (ts == "iso" || ts == "ISO") {
                    use_iso_timestamps = true;
                } else if (ts == "raw" || ts == "RAW") {
                    use_iso_timestamps = false;
                } else {
                    std::runtime_error err("invalid output statement (timestamp)");
                    BOOST_THROW_EXCEPTION(err);
                }
            } else if (kv.first == "format") {
                std::string fmt = kv.second.get_value<std::string>();
                if (fmt == "resp" || fmt == "RESP") {
                    output_format = RESP;
                } else if (fmt == "csv" || fmt == "CSV") {
                    output_format = CSV;
                } else {
                    std::runtime_error err("invalid output statement (format)");
                    BOOST_THROW_EXCEPTION(err);
                }
            }
        }
    }
    switch(output_format) {
    case RESP:
        formatter_.reset(new RESPOutputFormatter(session_, use_iso_timestamps));
        break;
    case CSV:
        formatter_.reset(new CSVOutputFormatter(session_, use_iso_timestamps));
        break;
    };

    _init_cursor();
}

void QueryResultsPooler::_init_cursor() {
    switch (endpoint_) {
    case ApiEndpoint::QUERY:
        cursor_ = session_->query(query_text_);
        break;
    case ApiEndpoint::SUGGEST:
        cursor_ = session_->suggest(query_text_);
        break;
    case ApiEndpoint::SEARCH:
        cursor_ = session_->search(query_text_);
        break;
    default:
        BOOST_THROW_EXCEPTION(std::runtime_error("Init-cursor failure, invalid endpoint"));
    };
}

void QueryResultsPooler::append(const char *data, size_t data_size) {
    throw_if_started();
    query_text_ += std::string(data, data + data_size);
}

aku_Status QueryResultsPooler::get_error() {
    aku_Status err = AKU_SUCCESS;
    if (cursor_->is_error(&err)) {
        return err;
    }
    return AKU_SUCCESS;
}

std::tuple<size_t, bool> QueryResultsPooler::read_some(char *buf, size_t buf_size) {
    aku_Status status = AKU_SUCCESS;
    throw_if_not_started();
    if (rdbuf_pos_ == rdbuf_top_) {
        if (cursor_->is_done()) {
            // This can be the case if error occured
            if (cursor_->is_error(&status)) {
                // Some error occured, put error message to the outgoing buffer and return
                int len = snprintf(buf, buf_size, "-%s\r\n", aku_error_message(status));
                if (len > 0) {
                    return std::make_tuple((size_t)len, true);
                }
            }
            return std::make_tuple(0u, true);
        }
        // read new data from DB
        rdbuf_top_ = cursor_->read(rdbuf_.data(), rdbuf_.size());
        rdbuf_pos_ = 0u;
        if (cursor_->is_error(&status)) {
            // Some error occured, put error message to the outgoing buffer and return
            int len = snprintf(buf, buf_size, "-%s\r\n", aku_error_message(status));
            if (len > 0) {
                return std::make_tuple((size_t)len, false);
            }
            return std::make_tuple(0u, false);
        }
    }

    // format output
    char* begin = buf;
    char* end = begin + buf_size;
    while(rdbuf_pos_ < rdbuf_top_) {
        const aku_Sample* sample = reinterpret_cast<const aku_Sample*>(rdbuf_.data() + rdbuf_pos_);
        char* next = formatter_->format(begin, end, *sample);
        if (next == nullptr) {
            // done
            break;
        }
        begin = next;
        assert(sample->payload.size);
        rdbuf_pos_ += sample->payload.size;
    }
    return std::make_tuple(begin - buf, false);
}

void QueryResultsPooler::close() {
    throw_if_not_started();
    cursor_->close();
}

QueryProcessor::QueryProcessor(std::weak_ptr<DbConnection> con, int rdbuf)
    : con_(con)
    , rdbufsize_(rdbuf)
{
    logger.info() << "QueryProcessor created";
}

QueryProcessor::~QueryProcessor() {
    logger.info() << "QueryProcessor destructed";
}

ReadOperation *QueryProcessor::create(ApiEndpoint endpoint) {
    auto con = con_.lock();
    if (con) {
        return new QueryResultsPooler(con->create_session(), rdbufsize_, endpoint);
    }
    std::runtime_error err("Database connection was closed");
    BOOST_THROW_EXCEPTION(err);
}

std::string QueryProcessor::get_all_stats() {
    auto con = con_.lock();
    if (con) {
        return con->get_all_stats();
    }
    std::runtime_error err("Database connection was closed");
    BOOST_THROW_EXCEPTION(err);
}

std::string QueryProcessor::get_resource(std::string name) {
    size_t outbufsize = 0x1000;
    char outbuf[outbufsize];
    aku_Status status = aku_get_resource(name.c_str(), outbuf, &outbufsize);
    if (status != AKU_SUCCESS) {
        return "-Invalid resource name";
    }
    return std::string(outbuf, outbuf + outbufsize);
}

}  // namespace

