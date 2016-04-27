#include "resp.h"
#include <boost/exception/all.hpp>
#include <cassert>

namespace Akumuli {

RESPError::RESPError(std::string msg, int pos)
    : StreamError(msg, pos)
{
}

RESPStream::RESPStream(ByteStreamReader *stream) {
    stream_ = stream;
}

RESPStream::Type RESPStream::next_type() const {
    auto ch = stream_->pick();
    Type result = BAD;
    switch(ch) {
    case '+':
        result = STRING;
        break;
    case ':':
        result = INTEGER;
        break;
    case '$':
        result = BULK_STR;
        break;
    case '*':
        result = ARRAY;
        break;
    case '-':
        result = ERROR;
        break;
    default:
        result = BAD;
        break;
    };
    return result;
}

u64 RESPStream::_read_int_body() {
    u64 result = 0;
    const int MAX_DIGITS = 84;  // Maximum number of decimal digits in u64
    int quota = MAX_DIGITS;
    while(quota) {
        Byte c = stream_->get();
        if (c == '\n') {
            // Note: I decided to support both \r\n and \n line endings in Akumuli for simplicity.
            return result;
        } else if (c == '\r') {
            c = stream_->get();
            if (c == '\n') {
                return result;
            }
            // Bad stream
            auto ctx = stream_->get_error_context("invalid symbol inside stream - '\\r'");
            BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        // c must be in [0x30:0x39] range
        if (c > 0x39 || c < 0x30) {
            auto ctx = stream_->get_error_context("can't parse integer (character value out of range)");
            BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        result = result*10 + static_cast<int>(c & 0x0F);
        quota--;
    }
    auto ctx = stream_->get_error_context("integer is too long");
    BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
}

u64 RESPStream::read_int() {
    Byte c = stream_->get();
    if (c != ':') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_int_body();
}

int RESPStream::_read_string_body(Byte *buffer, size_t byte_buffer_size) {
    auto p = buffer;
    int quota = std::min(byte_buffer_size, (size_t)RESPStream::STRING_LENGTH_MAX);
    while(quota) {
        Byte c = stream_->get();
        if (c == '\n') {
            return p - buffer;
        } else if (c == '\r') {
            c = stream_->get();
            if (c == '\n') {
                return p - buffer;
            } else {
                auto ctx = stream_->get_error_context("bad end of sequence");
                BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
            }
        }
        *p++ = c;
        quota--;
    }
    auto ctx = stream_->get_error_context("out of quota");
    BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
}

int RESPStream::read_string(Byte *buffer, size_t byte_buffer_size) {
    Byte c = stream_->get();
    if (c != '+') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_string_body(buffer, byte_buffer_size);
}

int RESPStream::read_bulkstr(Byte *buffer, size_t buffer_size) {
    Byte c = stream_->get();
    if (c != '$') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    // parse "{value}\r\n"
    u64 n = _read_int_body();
    if (n > RESPStream::BULK_LENGTH_MAX) {
        auto ctx = stream_->get_error_context("declared object size is too large");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    if (n > buffer_size) {
        // buffer is too small
        auto ctx = stream_->get_error_context("declared object size is too large");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    int nread = stream_->read(buffer, n);
    if (nread < 0) {
        // stream error
        return nread;
    }
    bool bad_eos = false;
    Byte cr = stream_->get();
    if (cr == '\r') {
        Byte lf = stream_->get();
        if (cr != '\r' || lf != '\n') {
            bad_eos = true;
        }
    } else if (cr != '\n') {
        bad_eos = true;
    }
    if (bad_eos) {
        auto ctx = stream_->get_error_context("bad end of stream");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return nread;
}

u64 RESPStream::read_array_size() {
    Byte c = stream_->get();
    if (c != '*') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_int_body();
}

}
