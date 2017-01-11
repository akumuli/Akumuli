#include "protocolparser.h"
#include <sstream>
#include <cassert>
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


// ReadBuffer class //

ReadBuffer::ReadBuffer(const size_t buffer_size)
    : BUFFER_SIZE(buffer_size)
    , buffer_(buffer_size*N_BUF, 0)
    , rpos_(0)
    , wpos_(0)
    , cons_(0)
    , buffers_allocated_(0)
{
}

Byte ReadBuffer::get() {
    if (rpos_ == wpos_) {
        auto ctx = get_error_context("unexpected end of stream");
        BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return buffer_[rpos_++];
}

Byte ReadBuffer::pick() const {
    if (rpos_ == wpos_) {
        auto ctx = get_error_context("unexpected end of stream");
        BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return buffer_[rpos_];
}

bool ReadBuffer::is_eof() {
    return rpos_ == wpos_;
}

int ReadBuffer::read(Byte *buffer, size_t buffer_len) {
    assert(buffer_len < 0x100000000ul);
    u32 to_read = wpos_ - rpos_;
    to_read = std::min(static_cast<u32>(buffer_len), to_read);
    std::copy(buffer_.begin() + rpos_, buffer_.begin() + rpos_ + to_read, buffer);
    rpos_ += to_read;
    return static_cast<int>(to_read);
}

int ReadBuffer::read_line(Byte* buffer, size_t quota) {
    assert(quota < 0x100000000ul);
    u32 available = wpos_ - rpos_;
    auto to_read = std::min(static_cast<u32>(quota), available);
    for (u32 i = 0; i < to_read; i++) {
        Byte c = buffer_[rpos_ + i];
        buffer[i] = c;
        if (c == '\n') {
            // Stop iteration
            u32 bytes_copied = i + 1;
            rpos_ += bytes_copied;
            return static_cast<int>(bytes_copied);
        }
    }
    // No end of line found
    return -1*static_cast<int>(to_read);
}

void ReadBuffer::close() {
}

std::tuple<std::string, size_t> ReadBuffer::get_error_context(const char *error_message) const {
    // Invariant: rpos_ points to bad symbol
    size_t position = 0;
    auto origin = buffer_.data() + cons_;
    const Byte* it = buffer_.data() + rpos_;
    const Byte* start = it;
    while(it > origin) {
        if (*it == '\n') {
            break;
        }
        start = it;
        it--;
        position++;
    }
    // Now start contains pointer to the begining of the bad line
    it = buffer_.data() + rpos_;
    const Byte* stop = it;
    const Byte* end = buffer_.data() + wpos_;
    while(it < end) {
        if (*it == '\r' || *it == '\n') {
            break;
        }
        stop = it;
        it++;
        if (it - start > StreamError::MAX_LENGTH) {
            break;
        }
    }
    auto err = std::string(start, stop);
    boost::algorithm::replace_all(err, "\r", "\\r");
    boost::algorithm::replace_all(err, "\n", "\\n");
    std::stringstream message;
    message << error_message << " - ";
    position += message.str().size();
    message << err;
    return std::make_tuple(message.str(), position);
}

void ReadBuffer::consume() {
    assert(buffers_allocated_ == 0);  // Invariant check: buffer can be invalidated!
    cons_ = rpos_;
}

void ReadBuffer::discard() {
    assert(buffers_allocated_ == 0);  // Invariant check: buffer can be invalidated!
    rpos_ = cons_;
}

ReadBuffer::BufferT ReadBuffer::pull() {
    assert(buffers_allocated_ == 0);  // Invariant check: buffer will be invalidated after vector.resize!
    buffers_allocated_++;

    u32 sz = static_cast<u32>(buffer_.size()) - wpos_;  // because previous push can bring partially filled buffer
    if (sz < BUFFER_SIZE) {
        if ((cons_ + sz) > BUFFER_SIZE) {
            // Problem can be solved by rotating the buffer and asjusting wpos_, rpos_ and cons_
            std::copy(buffer_.begin() + cons_, buffer_.end(), buffer_.begin());
            wpos_ -= cons_;
            rpos_ -= cons_;
            cons_ = 0;
        } else {
            // Double the size of the buffer
            buffer_.resize(buffer_.size() * 2);
        }
    }
    Byte* ptr = buffer_.data() + wpos_;
    return ptr;
}

void ReadBuffer::push(ReadBuffer::BufferT, u32 size) {
    assert(buffers_allocated_ == 1);
    buffers_allocated_--;
    wpos_ += size;
}


// ProtocolParser class //

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
                rdbuf_.discard();
                return;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    rdbuf_.discard();
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
                rdbuf_.discard();
                return;
            case RESPStream::INTEGER:
                std::tie(success, sample.timestamp) = stream.read_int();
                if (!success) {
                    rdbuf_.discard();
                    return;
                }
                break;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    rdbuf_.discard();
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
                rdbuf_.discard();
                return;
            case RESPStream::INTEGER:
                sample.payload.type = AKU_PAYLOAD_FLOAT;
                std::tie(success, sample.payload.float64) = stream.read_int();
                if (!success) {
                    rdbuf_.discard();
                    return;
                }
                sample.payload.size = sizeof(aku_Sample);
                break;
            case RESPStream::STRING:
                std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
                if (!success) {
                    rdbuf_.discard();
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
            rdbuf_.consume();
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
