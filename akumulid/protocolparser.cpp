#include "protocolparser.h"
#include <sstream>
#include <boost/algorithm/string.hpp>

#include "resp.h"
#include "ingestion_pipeline.h"

namespace Akumuli {


ProtocolParserError::ProtocolParserError(std::string line, size_t pos)
    : StreamError(line, pos)
{
}

DatabaseError::DatabaseError(aku_Status status)
    : std::exception()
    , status(status)
{
}

const char* DatabaseError::what() const noexcept {
    return aku_error_message(status);
}

ProtocolParser::ProtocolParser(std::shared_ptr<DbSession> consumer)
    : done_(false)
    , consumer_(consumer)
    , logger_("protocol-parser", 32)
{
}

void ProtocolParser::start() {
    logger_.info() << "Starting protocol parser";
}

void ProtocolParser::worker() {
    // Buffer to read strings from
    const int     buffer_len         = RESPStream::STRING_LENGTH_MAX;
    Byte          buffer[buffer_len] = {};
    int           bytes_read         = 0;
    // Data to read
    aku_Sample    sample;
    aku_Status    status = AKU_SUCCESS;
    //
    try {
        RESPStream stream(this);
        while(true) {
            bool success;
            // read id
            auto next = stream.next_type();
            switch(next) {
            case RESPStream::_AGAIN:
                discard();
                return;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    discard();
                    return;
                }
                status = consumer_->series_to_param_id(buffer, static_cast<size_t>(bytes_read), &sample);
                if (status != AKU_SUCCESS){
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context(aku_error_message(status));
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
                break;
            case RESPStream::INTEGER:
            case RESPStream::ARRAY:
            case RESPStream::BULK_STR:
            case RESPStream::ERROR:
            case RESPStream::_BAD:
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
            case RESPStream::_AGAIN:
                discard();
                return;
            case RESPStream::INTEGER:
                std::tie(success, sample.timestamp) = stream.read_int();
                if (!success) {
                    discard();
                    return;
                }
                break;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    discard();
                    return;
                }
                buffer[bytes_read] = '\0';
                if (aku_parse_timestamp(buffer, &sample) == AKU_SUCCESS) {
                    break;
                }
            case RESPStream::ARRAY:
            case RESPStream::BULK_STR:
            case RESPStream::ERROR:
            case RESPStream::_BAD:
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("unexpected parameter timestamp format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            };

            // read value
            next = stream.next_type();
            switch(next) {
            case RESPStream::_AGAIN:
                discard();
                return;
            case RESPStream::INTEGER:
                sample.payload.type = AKU_PAYLOAD_FLOAT;
                std::tie(success, sample.payload.float64) = stream.read_int();
                if (!success) {
                    discard();
                    return;
                }
                sample.payload.size = sizeof(aku_Sample);
                break;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    discard();
                    return;
                }
                if (bytes_read < 0) {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("array size can't be negative");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
                buffer[bytes_read] = '\0';
                sample.payload.type = AKU_PAYLOAD_FLOAT;
                sample.payload.float64 = strtod(buffer, nullptr);
                sample.payload.size = sizeof(aku_Sample);
                memset(buffer, 0, static_cast<size_t>(bytes_read));
                break;
            case RESPStream::ARRAY:
            case RESPStream::BULK_STR:
            case RESPStream::ERROR:
            case RESPStream::_BAD:
                // Bad frame
                {
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = get_error_context("unexpected parameter value format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            };
            status = consumer_->write(sample);
            // Message processed and frame can be removed (if possible)
            consume();
            if (status != AKU_SUCCESS) {
                BOOST_THROW_EXCEPTION(DatabaseError(status));
            }
        }
    } catch(EStopIteration const&) {
        logger_.info() << "EStopIteration";
        done_ = true;
    }
}

void ProtocolParser::parse_next(PDU pdu) {
    buffers_.push(pdu);
    worker();
}

Byte ProtocolParser::get() {
    while(true) {
        if (buffers_.empty()) {
            auto ctx = get_error_context("can't read from empty stream");
            BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        auto& top = buffers_.front();
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos++];
        }
        backlog_top();
        if (buffers_.empty()) {
            break;
        }
    }
    auto ctx = get_error_context("unexpected end of stream");
    BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
}

Byte ProtocolParser::pick() const {
    while(true) {
        if (buffers_.empty()) {
            auto ctx = get_error_context("can't read from empty stream");
            BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        auto& top = buffers_.front();
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos];
        }
        backlog_top();
        if (buffers_.empty()) {
            break;
        }
    }
    auto ctx = get_error_context("unexpected end of stream");
    BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
}

bool ProtocolParser::is_eof() {
    if (!buffers_.empty()) {
        auto& top = buffers_.front();
        if (top.pos < top.size) {
            return false;
        } else {
            // Top is consumed
            return buffers_.size() == 1;
        }
    }
    return true;
}

int ProtocolParser::read(Byte *buffer, size_t buffer_len) {
    int bytes_copied = 0;
    while(true) {
        if (buffers_.empty()) {
            return bytes_copied;
        }
        auto& top = buffers_.front();
        if (top.pos < top.size) {
            size_t sz = top.size - top.pos;
            size_t bytes_to_copy = std::min(sz, buffer_len);
            memcpy(buffer, top.buffer.get() + top.pos, bytes_to_copy);
            bytes_copied += static_cast<int>(bytes_to_copy);
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
        backlog_top();
    }
    return bytes_copied;
}

void ProtocolParser::close() {
    done_ = true;
}

std::tuple<std::string, size_t> ProtocolParser::get_error_from_pdu(PDU const& pdu) const {
    const char* origin = pdu.buffer.get();
    // Scan to PDU head
    if (pdu.pos == 0) {
        // Error in first symbol
        size_t size = std::min(pdu.size, static_cast<u32>(StreamError::MAX_LENGTH));
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
    auto delta = static_cast<u32>(begin - origin);  // PDU begining position from the origin
    auto size = pdu.size - delta;
    auto position = pdu_pos - delta;
    if (position < StreamError::MAX_LENGTH) {
        // Truncate string if it wouldn't hide error (most of the PDU's is small so
        // this will be almost always the case).
        size = std::min(size, static_cast<u32>(StreamError::MAX_LENGTH));
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

void ProtocolParser::consume() {
    if (buffers_.size()) {
        auto& top = buffers_.front();
        top.cons = top.pos;
    }
    std::queue<PDU> tmp;
    backlog_.swap(tmp);
}

void ProtocolParser::discard() {
    if (buffers_.size()) {
        auto& top = buffers_.front();
        top.pos = top.cons;
    }
    if (backlog_.size()) {
        while(!buffers_.empty()) {
            auto& top = buffers_.front();
            backlog_.push(top);
            buffers_.pop();
        }
        std::swap(buffers_, backlog_);
    }
}

void ProtocolParser::backlog_top() const {
    auto& top = buffers_.front();
    top.pos = top.cons;
    backlog_.push(top);
    buffers_.pop();
}

}
