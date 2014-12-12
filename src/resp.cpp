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

bool RESPStream::read_int(int *output) {
    Byte c = stream_->get();
    if (c != ':') {
        return false;
    }
    int result = 0;
    while(true) {
        c = stream_->get();
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
    }
}

size_t RESPStream::read_string(Byte *buffer, size_t byte_buffer_size) {
    throw "Not implemented";
}

size_t RESPStream::read_bulkstr(Byte *bufer, size_t buffer_size) {
    throw "Not implemented";
}

int RESPStream::read_array_size() {
    throw "Not implemented";
}

}
