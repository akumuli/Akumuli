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
    : rdbuf_(RDBUF_SIZE)
    , done_(false)
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
        RESPStream stream(&rdbuf_);
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
                    std::tie(msg, pos) = rdbuf_.get_error_context(aku_error_message(status));
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
                    std::tie(msg, pos) = rdbuf_.get_error_context("unexpected parameter id format");
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
                    std::tie(msg, pos) = rdbuf_.get_error_context("unexpected parameter timestamp format");
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
                    std::tie(msg, pos) = rdbuf_.get_error_context("array size can't be negative");
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
                    std::tie(msg, pos) = rdbuf_.get_error_context("unexpected parameter value format");
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

void ProtocolParser::parse_next(Byte* buffer, u32 sz) {
    rdbuf_.push(buffer, sz);
    worker();
}

Byte* ProtocolParser::get_next_buffer() {
    return rdbuf_.pull();
}

void ProtocolParser::close() {
    done_ = true;
}

}
