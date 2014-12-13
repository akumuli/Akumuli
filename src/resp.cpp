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

bool RESPStream::_read_int_body(uint64_t *output) {
    uint64_t result = 0;
    const int MAX_DIGITS = 84;  // Maximum number of decimal digits in uint64_t
    int quota = MAX_DIGITS;
    while(quota) {
        Byte c = stream_->get();
        if (c == '\r') {
            c = stream_->get();
            if (c == '\n') {
                *output = result;
                return true;
            }
            // Bad stream
            return false;
        }
        // c must be in [0x30:0x39] range
        if (c > 0x39 || c < 0x30) {
            return false;
        }
        result = result*10 + static_cast<int>(c & 0x0F);
        quota--;
    }
    // Integer is too long
    return false;
}

bool RESPStream::read_int(uint64_t *output) {
    Byte c = stream_->get();
    if (c != ':') {
        return false;
    }
    return _read_int_body(output);
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
                // bad end sequence
                return -1;
            }
        }
        *p++ = c;
        quota--;
    }
    // out of quota
    return -1;
}

int RESPStream::read_string(Byte *buffer, size_t byte_buffer_size) {
    Byte c = stream_->get();
    if (c != '+') {
        // bad call
        return -1;
    }
    return _read_string_body(buffer, byte_buffer_size);
}

int RESPStream::read_bulkstr(Byte *buffer, size_t buffer_size) {
    Byte c = stream_->get();
    if (c != '$') {
        // bad call
        return -1;
    }
    uint64_t n;
    // parse "{value}\r\n"
    if (!_read_int_body(&n)) {
        // can't read integer
        return -1;
    }
    if (n > RESPStream::BULK_LENGTH_MAX) {
        // declared object size is too large
        return -1;
    }
    if (n > buffer_size) {
        // buffer is too small
        return -1;
    }
    int nread = stream_->read(buffer, n);
    if (nread < 0) {
        // stream error
        return nread;
    }
    Byte cr = stream_->get();
    Byte lf = stream_->get();
    if (cr != '\r' || lf != '\n') {
        // bad end seq
        return -1;
    }
    return nread;
}

int RESPStream::read_array_size() {
    throw "Not implemented";
}

}
