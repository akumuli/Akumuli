#include "protocolparser.h"
#include "resp.h"

namespace Akumuli {

ProtocolParser::ProtocolParser()
    : stop_(false)
    , done_(false)
{
}

void ProtocolParser::start() {
    auto fn = std::bind(&ProtocolParser::worker, this, std::placeholders::_1);
    coroutine_.reset(new Coroutine(fn));
}

void ProtocolParser::worker(Caller& yield) {
    while(true) {
        if (buffers_.empty()) {
            yield();
            continue;
        }
        PDURef buf = std::move(buffers_.front());

        // TODO: concat with prev stream
    }
    done_ = true;
}

void ProtocolParser::parse_next(ProtocolParser::PDURef &&pdu) {
    buffers_.push(std::move(pdu));

    // yield control to worker
    coroutine_->operator()();
}

Byte ProtocolParser::get() {
    while(!buffers_.empty()) {
        auto top = buffers_.front().get();
        if (top->pos < top->size) {
            Byte* buf = top->buffer.get();
            return buf[top->pos++];
        }
        buffers_.pop();
    }
    throw StreamError("unexpected end of stream");
}

Byte ProtocolParser::pick() const {
    while(!buffers_.empty()) {
        auto top = buffers_.front().get();
        if (top->pos < top->size) {
            Byte* buf = top->buffer.get();
            return buf[top->pos++];
        }
        buffers_.pop();
    }
    throw StreamError("unexpected end of stream");
}

bool ProtocolParser::is_eof() {
    return done_;
}

int ProtocolParser::read(Byte *buffer, size_t buffer_len) {
    int bytes_copied = -1;
    while(!buffers_.empty()) {
        auto& top = buffers_.front();
        if (top->pos < top->size) {
            size_t sz = top->size - top->pos;
            size_t bytes_to_copy = std::min(sz, buffer_len);
            memcpy(buffer, top->buffer.get(), sz);
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

void ProtocolParser::close() {
}

}
