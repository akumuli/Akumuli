#include "stream.h"
#include <cassert>
#include <limits>
#include <cstring>  // for memcpy

namespace Akumuli {

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
    return 0;
}

Byte MemStreamReader::pick() const {
    if (pos_ < size_) {
        return buf_[pos_];
    }
    return 0;
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

bool MemStreamReader::get_error_if_any(int *error_code, std::string *message) {
    return false;
}

}
