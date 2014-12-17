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
    throw StreamError("unexpected end of stream");
}

Byte MemStreamReader::pick() const {
    if (pos_ < size_) {
        return buf_[pos_];
    }
    throw StreamError("unexpected end of stream");
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

// Memory stream combiner

MemoryStreamCombiner::MemoryStreamCombiner() {
}

void MemoryStreamCombiner::push(std::shared_ptr<Byte> buf, size_t len) {
    buffers_.push(std::make_tuple(buf, len, (size_t)0u));
}

static const int POS = 2;
static const int SIZE = 1;
static const int BUF = 0;

Byte MemoryStreamCombiner::get() {
    while(!buffers_.empty()) {
        auto& top = buffers_.front();
        if (std::get<POS>(top) < std::get<SIZE>(top)) {
            Byte* buf = std::get<BUF>(top).get();
            return buf[std::get<POS>(top)++];
        }
        buffers_.pop();
    }
    throw StreamError("unexpected end of stream");
}

Byte MemoryStreamCombiner::pick() const {
    while(!buffers_.empty()) {
        auto& top = buffers_.front();
        if (std::get<POS>(top) < std::get<SIZE>(top)) {
            Byte* buf = std::get<BUF>(top).get();
            return buf[std::get<POS>(top)];
        }
        buffers_.pop();
    }
    throw StreamError("unexpected end of stream");
}

bool MemoryStreamCombiner::is_eof() {
    return buffers_.empty();
}

int MemoryStreamCombiner::read(Byte *buffer, size_t buffer_len) {
    int bytes_copied = -1;
    while(!buffers_.empty()) {
        auto& top = buffers_.front();
        if (std::get<POS>(top) < std::get<SIZE>(top)) {
            size_t sz = std::get<SIZE>(top) - std::get<POS>(top);
            size_t bytes_to_copy = std::min(sz, buffer_len);
            memcpy(buffer, std::get<BUF>(top).get(), sz);
            bytes_copied += (int)bytes_to_copy;
            if (bytes_to_copy == buffer_len) {
                // everything is copied!
                break;
            } else {
                // continue reading from the next buffer in the queue
                buffer += bytes_to_copy;
                buffer_len -= bytes_to_copy;
            }
        }
        buffers_.pop();
    }
    return bytes_copied;
}

void MemoryStreamCombiner::close() {
}

}
