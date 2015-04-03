#include "protocolparser.h"
#include "resp.h"
#include <sstream>
#include <boost/algorithm/string.hpp>

namespace Akumuli {

ProtocolParserError::ProtocolParserError(std::string line, int pos)
    : StreamError(line, pos)
{
}

const PDU ProtocolParser::POISON_ = {
    std::shared_ptr<const Byte>(),
    0u, 0u
};

ProtocolParser::ProtocolParser(std::shared_ptr<ProtocolConsumer> consumer)
    : done_(false)
    , consumer_(consumer)
    , logger_("protocol-parser", 32)
{
}

void ProtocolParser::start() {
    logger_.info() << "Starting protocol parser";
    auto fn = std::bind(&ProtocolParser::worker, this, std::placeholders::_1);
    coroutine_.reset(new Coroutine(fn));
}

void ProtocolParser::worker(Caller& caller) {
    // Remember caller for use in ByteStreamReader's methods
    set_caller(caller);
    // Buffer to read strings from
    const int     buffer_len         = RESPStream::STRING_LENGTH_MAX;
    Byte          buffer[buffer_len] = {};
    int           bytes_read         = 0;
    // Data to read
    aku_ParamId   id                 = 0;
    std::string   sid;
    bool          integer_id         = false;
    aku_Timestamp ts                 = 0;
    double        value              =.0;
    //
    try {
        RESPStream stream(this);
        while(true) {
            // read id
            auto next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                id = stream.read_int();
                integer_id = true;
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                sid = std::string(buffer, buffer + bytes_read);
                integer_id = false;
                break;
            case RESPStream::BULK_STR:
                // Compressed chunk of data
                bytes_read = stream.read_bulkstr(buffer, buffer_len);
                consumer_->add_bulk_string(buffer, bytes_read);
                continue;
            default:
                // Bad frame
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("unexpected parameter id format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            };

            // read ts
            next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                ts = stream.read_int();
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                // TODO: parse date-time
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("not implemented");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            default:
                // Bad frame
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("Unexpected parameter timestamp format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            };

            // read value
            next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                value = stream.read_int();
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                value = strtod(buffer, nullptr);
                memset(buffer, 0, bytes_read);
                break;
            default:
                // Bad frame
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("Unexpected parameter value format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            };

            if (integer_id) {
                consumer_->write_double(id, ts, value);
            } else {
                // TODO: write blob
                BOOST_THROW_EXCEPTION(std::runtime_error("not implemented"));
            }
        }
    } catch(EStopIteration const&) {
        logger_.info() << "EStopIteration";
        done_ = true;
    }
}

void ProtocolParser::set_caller(Caller& caller) {
    caller_ = &caller;
}

void ProtocolParser::yield_to_worker() {
    coroutine_->operator()();
}

void ProtocolParser::yield_to_client() const {
    (*caller_)();
}

void ProtocolParser::throw_if_poisoned(PDU const& top) const {
    if (top.size == 0 && top.pos == 0) {
        throw EStopIteration();
    }
}

void ProtocolParser::parse_next(PDU pdu) {
    buffers_.push(pdu);
    yield_to_worker();
}

Byte ProtocolParser::get() {
    while(true) {
        if (buffers_.empty()) {
            yield_to_client();
        }
        auto& top = buffers_.front();
        throw_if_poisoned(top);
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos++];
        }
        buffers_.pop();
        yield_to_client();
    }
    auto ctx = get_error_context("unexpected end of stream");
    BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
}

Byte ProtocolParser::pick() const {
    while(true) {
        if (buffers_.empty()) {
            yield_to_client();
        }
        auto& top = buffers_.front();
        throw_if_poisoned(top);
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos];
        }
        buffers_.pop();
        yield_to_client();
    }
    auto ctx = get_error_context("unexpected end of stream");
    BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
}

bool ProtocolParser::is_eof() {
    return done_;
}

int ProtocolParser::read(Byte *buffer, size_t buffer_len) {
    int bytes_copied = 0;
    while(true) {
        if (buffers_.empty()) {
            yield_to_client();
        }
        auto& top = buffers_.front();
        if (top.pos < top.size) {
            size_t sz = top.size - top.pos;
            size_t bytes_to_copy = std::min(sz, buffer_len);
            memcpy(buffer, top.buffer.get() + top.pos, bytes_to_copy);
            bytes_copied += (int)bytes_to_copy;
            top.pos += bytes_to_copy;
            if (bytes_to_copy == buffer_len) {
                // everything is copied!
                break;
            } else {
                // continue reading from the next buffer in the queue
                buffer += bytes_to_copy;
                buffer_len -= bytes_to_copy;
            }
        }
        buffers_.pop();
    }
    return bytes_copied;
}

void ProtocolParser::close() {
    buffers_.push(POISON_);
    // stop worker
    yield_to_worker();
}

std::tuple<std::string, size_t> ProtocolParser::get_error_from_pdu(PDU const& pdu) const {
    const char* origin = pdu.buffer.get();
    // Scan to PDU head
    if (pdu.pos == 0) {
        // Error in first symbol
        size_t size = std::min(pdu.size, (size_t)StreamError::MAX_LENGTH);
        auto res = std::string(origin, origin + size);
        boost::algorithm::replace_all(res, "\r", "\\r");
        boost::algorithm::replace_all(res, "\n", "\\n");
        return std::make_pair(res, 0);
    }
    auto pdu_pos = pdu.pos;
    auto ipos = origin + pdu_pos;   // Iterator
    auto begin = origin;            // PDU begining
    while (ipos > origin) {
        if (*ipos == '\n') {        // Stop when prev. PDU begining or origin was reached
            break;
        }
        begin = ipos;
        ipos--;
    }
    auto delta = (begin - origin);       // PDU begining position from the origin
    auto size = pdu.size - delta;
    auto position = pdu_pos - delta;
    if (position < StreamError::MAX_LENGTH) {
        // Truncate string if it wouldn't hide error (most of the PDU's is small so
        // this will be almost always the case).
        size = std::min(size, (size_t)StreamError::MAX_LENGTH);
    }
    auto res = std::string(begin, begin + size);
    boost::algorithm::replace_all(res, "\r", "\\r");
    boost::algorithm::replace_all(res, "\n", "\\n");
    return std::make_pair(res, position);
}

std::tuple<std::string, size_t> ProtocolParser::get_error_context(const char* msg) const {
    if (buffers_.empty()) {
        return std::make_tuple(std::string("Can't generate error, no data"), 0u);
    }
    auto& top = buffers_.front();
    std::string err;
    size_t pos;
    std::tie(err, pos) = get_error_from_pdu(top);
    std::stringstream message;
    message << msg << " - ";
    pos += message.str().size();
    message << err;
    return std::make_tuple(message.str(), pos);
}

}
