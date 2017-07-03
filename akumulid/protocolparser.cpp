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

RESPProtocolParser::RESPProtocolParser(std::shared_ptr<DbSession> consumer)
    : done_(false)
    , rdbuf_(RDBUF_SIZE)
    , consumer_(consumer)
    , logger_("resp-protocol-parser", 32)
{
}

void RESPProtocolParser::start() {
    logger_.info() << "Starting protocol parser";
}

bool RESPProtocolParser::parse_timestamp(RESPStream& stream, aku_Sample& sample) {
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

int RESPProtocolParser::parse_ids(RESPStream& stream, aku_ParamId* ids, int nvalues) {
    bool success;
    int bytes_read;
    int rowwidth = -1;
    const int buffer_len = RESPStream::STRING_LENGTH_MAX;
    Byte buffer[buffer_len] = {};
    // read id
    auto next = stream.next_type();
    switch(next) {
    case RESPStream::_AGAIN:
        rdbuf_.discard();
        return -1;
    case RESPStream::STRING:
        std::tie(success, bytes_read) = stream.read_string(buffer, buffer_len);
        if (!success) {
            rdbuf_.discard();
            return -1;
        }
        rowwidth = consumer_->name_to_param_id_list(buffer, buffer + bytes_read, ids, nvalues);
        if (rowwidth <= 0) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("invalid series name format");
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
    return rowwidth;
}

bool RESPProtocolParser::parse_values(RESPStream& stream, double* values, int nvalues) {
    const size_t buflen = 64;
    Byte buf[buflen];
    int bytes_read;
    int arrsize;
    bool success;
    auto parse_int_value = [&](int at) {
        std::tie(success, values[0]) = stream.read_int();
        if (!success) {
            return false;
        }
        return true;
    };
    auto parse_string_value = [&](int at) {
        std::tie(success, bytes_read) = stream.read_string(buf, buflen);
        if (!success) {
            return false;
        }
        if (bytes_read < 0) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("floating point value can't be that big");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        buf[bytes_read] = '\0';
        char* endptr = nullptr;
        values[at] = strtod(buf, &endptr);
        if (endptr - buf != bytes_read) {
            std::stringstream fmt;
            fmt << "can't parse double value: " << buf;
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context(fmt.str().c_str());
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        return true;
    };
    auto next = stream.next_type();
    switch(next) {
    case RESPStream::_AGAIN:
        return false;
    case RESPStream::INTEGER:
        if (nvalues == 1) {
            if (!parse_int_value(0)) {
                return false;
            }
        } else {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("array expected (bulk format), integer found");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        break;
    case RESPStream::STRING:
        // Single integer value returned
        if (nvalues == 1) {
            if (!parse_string_value(0)) {
                return false;
            }
        } else {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("array expected (bulk format), string found");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        break;
    case RESPStream::ARRAY:
        std::tie(success, arrsize) = stream.read_array_size();
        if (!success) {
            return false;
        }
        if (arrsize != nvalues) {
            std::string msg;
            size_t pos;
            const char* error;
            if (arrsize < nvalues) {
                error = "wrong array size, more values expected";
            } else {
                error = "wrong array size, less values expected";
            }
            std::tie(msg, pos) = rdbuf_.get_error_context(error);
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        for (int i = 0; i < arrsize; i++) {
            next = stream.next_type();
            switch(next) {
                case RESPStream::_AGAIN:
                    return false;
                case RESPStream::INTEGER:
                    if (!parse_int_value(i)) {
                        return false;
                    }
                    break;
                case RESPStream::STRING:
                    if (!parse_string_value(i)) {
                        return false;
                    }
                    break;
                case RESPStream::ARRAY:
                case RESPStream::BULK_STR:
                case RESPStream::ERROR:
                case RESPStream::_BAD: {
                    // Bad frame
                    std::string msg;
                    size_t pos;
                    std::tie(msg, pos) = rdbuf_.get_error_context("unexpected parameter value format");
                    BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
                }
            }
        }
        break;
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

void RESPProtocolParser::worker() {
    // Buffer to read strings from
    u64 paramids[AKU_LIMITS_MAX_ROW_WIDTH];
    double values[AKU_LIMITS_MAX_ROW_WIDTH];
    int rowwidth = 0;
    // Data to read
    aku_Sample sample;
    aku_Status status = AKU_SUCCESS;
    //
    RESPStream stream(&rdbuf_);
    while(true) {
        bool success;
        // read id
        rowwidth = parse_ids(stream, paramids, AKU_LIMITS_MAX_ROW_WIDTH);
        if (rowwidth < 0) {
            rdbuf_.discard();
            return;
        }
        // read ts
        success = parse_timestamp(stream, sample);
        if (!success) {
            rdbuf_.discard();
            return;
        }
        success = parse_values(stream, values, rowwidth);
        if (!success) {
            rdbuf_.discard();
            return;
        }

        rdbuf_.consume();

        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.payload.size = sizeof(aku_Sample);
        // Timestamp is initialized once and for all
        for (int i = 0; i < rowwidth; i++) {
            sample.paramid = paramids[i];
            sample.payload.float64 = values[i];
            status = consumer_->write(sample);
            // Message processed and frame can be removed (if possible)
            if (status != AKU_SUCCESS) {
                BOOST_THROW_EXCEPTION(DatabaseError(status));
            }
        }
    }
}

void RESPProtocolParser::parse_next(Byte* buffer, u32 sz) {
    rdbuf_.push(buffer, sz);
    worker();
}

Byte* RESPProtocolParser::get_next_buffer() {
    return rdbuf_.pull();
}

void RESPProtocolParser::close() {
    done_ = true;
}


//     OpenTSDB protocol      //

OpenTSDBProtocolParser::OpenTSDBProtocolParser(std::shared_ptr<DbSession> consumer)
    : done_(false)
    , rdbuf_(RDBUF_SIZE)
    , consumer_(consumer)
    , logger_("opentsdb-protocol-parser", 32)
{
}

void OpenTSDBProtocolParser::start() {
    logger_.info() << "Starting protocol parser";
}

void OpenTSDBProtocolParser::parse_next(Byte* buffer, u32 sz) {
    rdbuf_.push(buffer, sz);
    worker();
}

Byte* OpenTSDBProtocolParser::get_next_buffer() {
    return rdbuf_.pull();
}

void OpenTSDBProtocolParser::close() {
    done_ = true;
}

void OpenTSDBProtocolParser::worker() {
    const size_t buffer_len = AKU_LIMITS_MAX_SNAME + 3 + 17 + 26;  // 3 space delimiters + 17 for value + 26 for timestampm
    Byte buffer[buffer_len];
    while(true) {
        aku_Sample sample;
        aku_Status status = AKU_SUCCESS;
        int len = rdbuf_.read_line(buffer, buffer_len);
        if (len <= 0) {
            // Buffer don't have a full PDU
            return;
        }
        Byte* pbuf = buffer;
        Byte const* pend = buffer + len;
        // Find series name in the buffer
        int quota = len;
        bool skip = true;
        while(quota--) {
            int ix = len - quota;
            Byte c = buffer[ix];
            if (c == ',') {
                buffer[ix] = ' ';
            } else if (c == ' ') {
                if (skip) {  // we should skip first space character that delimits metric name from tags
                    skip = false;
                } else {
                    break;
                }
            }
        }
        if (quota == 0) {
            // Invalid series name
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("invalid series name, can't find the end");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        // Buffer contains only one data point
        status = consumer_->series_to_param_id(pbuf, static_cast<u32>(len - quota), &sample);
        if (status != AKU_SUCCESS) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("invalid series name format");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        pbuf += static_cast<u32>(len - quota);

        // Read timestamp
        while(pbuf < pend && *pbuf == ' ') {
            pbuf++;
        }

        if (pbuf >= pend) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("unexpected end of PDU");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }

        len = static_cast<int>(pend - pbuf);
        quota = len;

        while(quota--) {
            int ix = len - quota;
            Byte c = pbuf[ix];
            if (c == ' ') {
                break;
            }
        }
        if (quota == 0) {
            // Invalid timestamp
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("invalid timestamp, can't find the end");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }
        status = aku_parse_timestamp(pbuf, &sample);
        if (status != AKU_SUCCESS) {
            bool err = false;
            if (status == AKU_EBAD_ARG) {
                // Try to parse as int
                Byte* endptr;
                pbuf[len - quota] = '\0';
                auto result = strtol(pbuf, &endptr, 10);
                pbuf[len - quota] = ' ';
                if (result == 0) {
                    err = true;
                }
                sample.timestamp = result;
            } else {
                err = true;
            }
            if (err) {
                std::string msg;
                size_t pos;
                std::tie(msg, pos) = rdbuf_.get_error_context("invalid timestamp format");
                BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
            }
        }
        pbuf += static_cast<u32>(len - quota);

        // Read value
        while(pbuf < pend && *pbuf == ' ') {
            pbuf++;
        }

        if (pbuf >= pend) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("unexpected end of PDU");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }

        len = static_cast<int>(pend - pbuf);
        quota = len - 1;

        pbuf[quota] = '\0';
        char* endptr = nullptr;
        double value = strtod(pbuf, &endptr);
        if (endptr - pbuf != quota) {
            std::string msg;
            size_t pos;
            std::tie(msg, pos) = rdbuf_.get_error_context("bad floating point value");
            BOOST_THROW_EXCEPTION(ProtocolParserError(msg, pos));
        }

        sample.payload.float64 = value;
        sample.payload.type = AKU_PAYLOAD_FLOAT;

        // Put value
        status = consumer_->write(sample);
        if (status != AKU_SUCCESS) {
            BOOST_THROW_EXCEPTION(DatabaseError(status));
        }

        rdbuf_.consume();
    }
}

}
