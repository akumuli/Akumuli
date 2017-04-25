#include "stream.h"
#include <cassert>
#include <limits>
#include <cstring>  // for memcpy
#include <sstream>
#include <boost/exception/all.hpp>

namespace Akumuli {

StreamError::StreamError(std::string line, size_t pos)
    : line_(line)
    , pos_(pos)
{
}

const char* StreamError::what() const noexcept {
    return line_.c_str();
}

std::string StreamError::get_bottom_line() const {
    std::stringstream s;
    for (int i = 0; i < (static_cast<int>(pos_)-1); i++) {
        s << ' ';
    }
    s << '^';
    return s.str();
}

ByteStreamReader::~ByteStreamReader() {}

// MemStreamReader implementation

MemStreamReader::MemStreamReader(const Byte *buffer, size_t buffer_len)
    : buf_(buffer)
    , size_(buffer_len)
    , cons_(0ul)
    , pos_(0ul)
{
    assert(size_ < std::numeric_limits<int>::max());
}

Byte MemStreamReader::get() {
    if (pos_ < size_) {
        return buf_[pos_++];
    }
    BOOST_THROW_EXCEPTION(StreamError("unexpected end of stream", pos_));
}

Byte MemStreamReader::pick() const {
    if (pos_ < size_) {
        return buf_[pos_];
    }
    BOOST_THROW_EXCEPTION(StreamError("unexpected end of stream", pos_));
}

bool MemStreamReader::is_eof() {
    return pos_ == size_;
}

int MemStreamReader::read(Byte *buffer, size_t buffer_len) {
    auto nbytes = std::min(buffer_len, size_ - pos_);
    memcpy(buffer, buf_ + pos_, nbytes);
    pos_ += nbytes;
    return static_cast<int>(nbytes);
}

int MemStreamReader::read_line(Byte* buffer, size_t quota) {
    auto available = size_ - pos_;
    auto to_read = std::min(quota, available);
    for (u32 i = 0; i < to_read; i++) {
        Byte c = buf_[pos_ + i];
        buffer[i] = c;
        if (c == '\n') {
            // Stop iteration
            u32 bytes_copied = i + 1;
            pos_ += bytes_copied;
            return static_cast<int>(bytes_copied);
        }
    }
    // No end of line found
    return -1*static_cast<int>(to_read);
}

void MemStreamReader::close() {
    pos_ = size_;
}

std::tuple<std::string, size_t> MemStreamReader::get_error_context(const char* error_message) const {
    return std::make_tuple(error_message, 0u);
}

void MemStreamReader::consume() {
    cons_ = pos_;
}

void MemStreamReader::discard() {
    pos_ = cons_;
}

}
