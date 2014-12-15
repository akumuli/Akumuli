#include "resp.h"
#include <cassert>

namespace Akumuli {

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

uint64_t RESPStream::_read_int_body() {
    uint64_t result = 0;
    const int MAX_DIGITS = 84;  // Maximum number of decimal digits in uint64_t
    int quota = MAX_DIGITS;
    while(quota) {
        Byte c = stream_->get();
        if (c == '\r') {
            c = stream_->get();
            if (c == '\n') {
                return result;
            }
            // Bad stream
            throw RESPError("invalid symbol inside stream - '\\r'");
        }
        // c must be in [0x30:0x39] range
        if (c > 0x39 || c < 0x30) {
            throw RESPError("can't parse integer (character value out of range)");
        }
        result = result*10 + static_cast<int>(c & 0x0F);
        quota--;
    }
    throw RESPError("integer is too long");
}

uint64_t RESPStream::read_int() {
    Byte c = stream_->get();
    if (c != ':') {
        throw RESPError("bad call");
    }
    return _read_int_body();
}

int RESPStream::_read_string_body(Byte *buffer, size_t byte_buffer_size) {
    auto p = buffer;
    int quota = std::min(byte_buffer_size, (size_t)RESPStream::STRING_LENGTH_MAX);
    while(quota) {
        Byte c = stream_->get();
        if (c == '\r') {
            c = stream_->get();
            if (c == '\n') {
                return p - buffer;
            } else {
                throw RESPError("bad end of sequence");
            }
        }
        *p++ = c;
        quota--;
    }
    throw RESPError("out of quota");
}

int RESPStream::read_string(Byte *buffer, size_t byte_buffer_size) {
    Byte c = stream_->get();
    if (c != '+') {
        throw RESPError("bad call");
    }
    return _read_string_body(buffer, byte_buffer_size);
}

int RESPStream::read_bulkstr(Byte *buffer, size_t buffer_size) {
    Byte c = stream_->get();
    if (c != '$') {
        throw RESPError("bad call");
    }
    // parse "{value}\r\n"
    uint64_t n = _read_int_body();
    if (n > RESPStream::BULK_LENGTH_MAX) {
        throw RESPError("declared object size is too large");
    }
    if (n > buffer_size) {
        // buffer is too small
        throw RESPError("declared object size is too large");
    }
    int nread = stream_->read(buffer, n);
    if (nread < 0) {
        // stream error
        return nread;
    }
    Byte cr = stream_->get();
    Byte lf = stream_->get();
    if (cr != '\r' || lf != '\n') {
        throw RESPError("bad end of stream");
    }
    return nread;
}

uint64_t RESPStream::read_array_size() {
    Byte c = stream_->get();
    if (c != '*') {
        throw RESPError("bad call");
    }
    return _read_int_body();
}

}
