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
    // Get the frame: [...\r\n...\r\n...\r\n]
    auto origin = buffer_.data() + cons_;
    const Byte* stop = origin;
    int nlcnt = 0;
    const Byte* end = buffer_.data() + wpos_;
    while (stop < end) {
        if (*stop == '\n') {
            nlcnt++;
            if (nlcnt == 3) {
                break;
            }
        }
        stop++;
    }
    auto err = std::string(origin, stop);
    boost::algorithm::replace_all(err, "\r", "\\r");
    boost::algorithm::replace_all(err, "\n", "\\n");
    std::stringstream message;
    message << error_message << " - ";
    message << err;
    return std::make_tuple(message.str(), 0);
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

bool ProtocolParser::parse_timestamp(RESPStream& stream, aku_Sample& sample) {
    bool success = false;
    int bytes_read = 0;
    const size_t tsbuflen = 28;
    Byte tsbuf[tsbuflen];
    auto next = stream.next_type();
    switch(next) {
    case RESPStream::_AGAIN:
        return false;
    case RESPStream::INTEGER:
        std::tie(success, sample.timestamp) = stream.read_int();
        if (!success) {
            return false;
        }
        break;
    case RESPStream::STRING:
        std::tie(success, bytes_read) = stream.read_string(tsbuf, tsbuflen);
        if (!success) {
            return false;
        }
        tsbuf[bytes_read] = '\0';
        if (aku_parse_timestamp(tsbuf, &sample) == AKU_SUCCESS) {
            break;
        }
        // Fail through on error
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
    return true;
}

bool ProtocolParser::parse_value(RESPStream& stream, aku_Sample& sample) {
    const size_t buflen = 30;
    Byte buf[buflen];
    int bytes_read;
    bool success;
    auto next = stream.next_type();
    switch(next) {
    case RESPStream::_AGAIN:
        return false;
    case RESPStream::INTEGER:
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        std::tie(success, sample.payload.float64) = stream.read_int();
        if (!success) {
            return false;
        }
        sample.payload.size = sizeof(aku_Sample);
        break;
    case RESPStream::STRING:
        std::tie(success, bytes_read) = stream.read_string(buf, buflen);
        if (!success) {
            return false;
        }
        if (bytes_read < 0) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("array size can't be negative");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        buf[bytes_read] = '\0';
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.payload.float64 = strtod(buf, nullptr);
        sample.payload.size = sizeof(aku_Sample);
        memset(buf, 0, static_cast<size_t>(bytes_read));
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
    return true;
}

bool ProtocolParser::parse_bulk_format(RESPStream& stream, Byte* buffer, size_t buffer_len) {
    /* Bulk load format:
       *N\r\n
       +tag1=value1 tag2=value2\r\n
       +20161201T003011.000011111\r\n
       +metric1\r\n
       +11.11\r\n
       +metric2\r\n
       +22.22\r\n
    */
    assert(buffer_len == (RESPStream::METRIC_LENGTH_MAX + RESPStream::STRING_LENGTH_MAX + 1));
    aku_Sample sample = {};
    // Read number of elements
    bool success;
    u64 arrsize;
    std::tie(success, arrsize) = stream.read_array_size();
    if (!success) {
        std::string msg;
        size_t pos;
        std::tie(msg, pos) = rdbuf_.get_error_context("can't read array size");
        BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
    }
    // Read tagline
    int tagline_len;
    Byte* tagline = buffer + RESPStream::METRIC_LENGTH_MAX + 1;
    tagline[-1] = ' ';
    std::tie(success, tagline_len) = stream.read_string(tagline, buffer_len - RESPStream::METRIC_LENGTH_MAX);
    if (!success) {
        return false;
    }
    // Read timestamp
    if (!parse_timestamp(stream, sample)) {
        return false;
    }
    // Read metric names and values
    for (u32 i = 0; i < arrsize; i++) {
        // Read metric name
        int bytes_read;
        Byte tmp[RESPStream::METRIC_LENGTH_MAX];
        std::tie(success, bytes_read) = stream.read_string(tmp, RESPStream::METRIC_LENGTH_MAX);
        if (!success) {
            return false;
        }
        // Read value
        if (!parse_value(stream, sample)) {
            return false;
        }
        // Copy metric name and translate
        Byte* sname = tagline - bytes_read;
        memcpy(sname, tmp, static_cast<size_t>(bytes_read));

        auto status = consumer_->series_to_param_id(sname, static_cast<size_t>(bytes_read + tagline_len), &sample);
        if (status != AKU_SUCCESS){
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context(aku_error_message(status));
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }

        // Write datapoint to storage
        status = consumer_->write(sample);
        if (status != AKU_SUCCESS) {
            BOOST_THROW_EXCEPTION(DatabaseError(status));
        }
    }
    return true;
}

void ProtocolParser::worker() {
    // Buffer to read strings from
    const int buffer_len = RESPStream::STRING_LENGTH_MAX + RESPStream::METRIC_LENGTH_MAX + 1;
    Byte buffer[buffer_len] = {};
    int bytes_read = 0;
    // Data to read
    aku_Sample sample;
    aku_Status status = AKU_SUCCESS;
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
                if (!parse_bulk_format(stream, buffer, buffer_len)) {
                    rdbuf_.discard();
                    return;
                }
                rdbuf_.consume();
                break;
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
            success = parse_timestamp(stream, sample);
            if (!success) {
                rdbuf_.discard();
                return;
            }
            // read value
            success = parse_value(stream, sample);
            if (!success) {
                rdbuf_.discard();
                return;
            }
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
