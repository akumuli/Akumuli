#include "stream.h"
#include <cassert>
#include <limits>
#include <cstring>  // for memcpy

namespace Akumuli {

StreamError::StreamError(std::string line, int pos)
    : line_(line)
    , pos_(pos)
{
}

const char* StreamError::what() const throw() {
    return line_.c_str();
}

std::string StreamError::get_bottom_line() const {
    return std::string(static_cast<size_t>(pos_), ' ');
}

ByteStreamReader::~ByteStreamReader() {}

// MemStreamReader implementation

MemStreamReader::MemStreamReader(const Byte *buffer, size_t buffer_len)
    : buf_(buffer)
    , size_(buffer_len)
    , pos_(0ul)
{
    assert(size_ < std::numeric_limits<int>::max());
}

Byte MemStreamReader::get() {
    if (pos_ < size_) {
        return buf_[pos_++];
    }
    throw StreamError("unexpected end of stream", pos_);
}

Byte MemStreamReader::pick() const {
    if (pos_ < size_) {
        return buf_[pos_];
    }
    throw StreamError("unexpected end of stream", pos_);
}

bool MemStreamReader::is_eof() {
    return pos_ == size_;
}

int MemStreamReader::read(Byte *buffer, size_t buffer_len) {
    int nbytes = static_cast<int>(std::min(buffer_len, size_ - pos_));
    memcpy(buffer, buf_ + pos_, nbytes);
    pos_ += nbytes;
    return nbytes;
}

void MemStreamReader::close() {
    pos_ = size_;
}

std::tuple<std::string, size_t> MemStreamReader::get_error_context(const char* error_message) const {
    return std::make_tuple(error_message, 0u);
}

}
