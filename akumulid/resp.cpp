#include "resp.h"
#include <boost/exception/all.hpp>
#include <cassert>

namespace Akumuli {

RESPError::RESPError(std::string msg, size_t pos)
    : StreamError(msg, pos)
{
}

RESPStream::RESPStream(ByteStreamReader *stream) {
    stream_ = stream;
}

RESPStream::Type RESPStream::next_type() const {
    if (stream_->is_eof()) {
        return _AGAIN;
    }
    auto ch = stream_->pick();
    Type result = _BAD;
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
        result = _BAD;
        break;
    };
    return result;
}

std::tuple<bool, u64> RESPStream::_read_int_body() {
    const int MAX_DIGITS = 84 + 2;  // Maximum number of decimal digits in u64 + \r\n
    Byte buf[MAX_DIGITS];
    u64 result = 0;
    int res = stream_->read_line(buf, MAX_DIGITS);
    if (res <= 0) {
        if (res == -1*MAX_DIGITS) {
            // Invalid input, too many digits in the number
            auto ctx = stream_->get_error_context("integer is too long");
            BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        return std::make_tuple(false, 0ull);
    }
    for (int i = 0; i < res; i++) {
        Byte c = buf[i];
        // c must be in [0x30:0x39] range
        if (c <= 0x39 && c >= 0x30) {
            result = result*10 + static_cast<u32>(c & 0x0F);
        } else if (c == '\n') {
            // Note: I decided to support both \r\n and \n line endings in Akumuli for simplicity.
            return std::make_tuple(true, result);
        } else if (c == '\r') {
            // The next one should be \n
            i++;
            if (i < res && buf[i] == '\n') {
                return std::make_tuple(true, result);
            }
            auto ctx = stream_->get_error_context("invalid symbol inside stream - '\\r'");
            BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
        } else {
            auto ctx = stream_->get_error_context("can't parse integer (character value out of range)");
            BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
        }
    }
    // Bad stream
    auto ctx = stream_->get_error_context("error in stream decoding routine");
    BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
}

std::tuple<bool, u64> RESPStream::read_int() {
    if (stream_->is_eof()) {
        return std::make_tuple(false, 0ull);
    }
    Byte c = stream_->get();
    if (c != ':') {
        auto ctx = stream_->get_error_context("integer expected");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_int_body();
}

std::tuple<bool, int> RESPStream::_read_string_body(Byte *buffer, size_t byte_buffer_size) {
    auto quota = std::min(byte_buffer_size, static_cast<size_t>(RESPStream::STRING_LENGTH_MAX));
    auto res = stream_->read_line(buffer, quota);
    if (res > 0) {
        // Success
        if (buffer[res - 1] == '\n') {
            res--;
            if (buffer[res - 1] == '\r') {
                res--;
            }
        }
        return std::make_tuple(true, res);
    }
    if (res == -1*static_cast<int>(quota)) {
        // Max string length reached, invalid input.
        auto ctx = stream_->get_error_context("out of quota");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return std::make_tuple(false, 0ull);
}

std::tuple<bool, int> RESPStream::read_string(Byte *buffer, size_t byte_buffer_size) {
    if (stream_->is_eof()) {
        return std::make_tuple(false, 0ull);
    }
    Byte c = stream_->get();
    if (c != '+') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_string_body(buffer, byte_buffer_size);
}

std::tuple<bool, int> RESPStream::read_bulkstr(Byte *buffer, size_t buffer_size) {
    if (stream_->is_eof()) {
        return std::make_tuple(false, 0);
    }
    Byte c = stream_->get();
    if (c != '$') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    // parse "{value}\r\n"
    bool success;
    u64 n;
    std::tie(success, n) = _read_int_body();
    if (!success) {
        return std::make_tuple(false, 0);
    }
    if (n > RESPStream::BULK_LENGTH_MAX) {
        auto ctx = stream_->get_error_context("declared object size is too large");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    if (n > buffer_size) {
        // buffer is too small
        return std::make_tuple(false, -1*static_cast<int>(n));
    }
    int nread = stream_->read(buffer, n);
    if (nread < static_cast<int>(n)) {  // Safe to cast this way because n <= BULK_LENGTH_MAX
        // stream error
        return std::make_tuple(false, nread);
    }
    bool bad_eos = false;
    if (stream_->is_eof()) {
        return std::make_tuple(false, 0);
    }
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
    return std::make_tuple(true, nread);
}

std::tuple<bool, u64> RESPStream::read_array_size() {
    if (stream_->is_eof()) {
        return std::make_tuple(false, 0);
    }
    Byte c = stream_->get();
    if (c != '*') {
        auto ctx = stream_->get_error_context("bad call");
        BOOST_THROW_EXCEPTION(RESPError(std::get<0>(ctx), std::get<1>(ctx)));
    }
    return _read_int_body();
}

}
