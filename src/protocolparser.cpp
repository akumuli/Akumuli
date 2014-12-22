#include "protocolparser.h"
#include "resp.h"

namespace Akumuli {

const PDU ProtocolParser::POISON_ = {
    std::shared_ptr<const Byte>(),
    0u, 0u
};

ProtocolParser::ProtocolParser(std::shared_ptr<ProtocolConsumer> consumer)
    : done_(false)
    , consumer_(consumer)
{
}

void ProtocolParser::start() {
    auto fn = std::bind(&ProtocolParser::worker, this, std::placeholders::_1);
    coroutine_.reset(new Coroutine(fn));
}

void ProtocolParser::worker(Caller& caller) {
    // Remember caller for use in ByteStreamReader's methods
    set_caller(caller);
    // Buffer to read strings from
    const int buffer_len = RESPStream::STRING_LENGTH_MAX;
    Byte buffer[buffer_len] = {};
    int bytes_read = 0;
    // Data to read
    aku_ParamId id;
    std::string sid;
    bool integer_id = false;
    aku_TimeStamp ts;
    double value;
    //
    try {
        RESPStream stream(this);
        while(true) {
            // read id
            auto next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                id = stream.read_int();
                integer_id = true;
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                sid = std::string(buffer, buffer + bytes_read);
                integer_id = false;
                break;
            default:
                // Bad frame
                throw ProtocolParserError("Unexpected parameter id format");
                break;
            };

            // read ts
            next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                ts = stream.read_int();
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                // TODO: parse date-time
                throw "Not implemented";
                break;
            default:
                // Bad frame
                throw ProtocolParserError("Unexpected parameter timestamp format");
                break;
            };

            // read value
            next = stream.next_type();
            switch(next) {
            case RESPStream::INTEGER:
                value = stream.read_int();
                break;
            case RESPStream::STRING:
                bytes_read = stream.read_string(buffer, buffer_len);
                value = strtod(buffer, nullptr);
                memset(buffer, 0, bytes_read);
                break;
            default:
                // Bad frame
                throw ProtocolParserError("Unexpected parameter value format");
                break;
            };

            if (integer_id) {
                consumer_->write_double(id, ts, value);
            } else {
                // TODO:
                throw "Not implemented";
            }
        }
    } catch(EStopIteration const&) {
        done_ = true;
    }
}

void ProtocolParser::set_caller(Caller& caller) {
    caller_ = &caller;
}

void ProtocolParser::yield_to_worker() {
    coroutine_->operator()();
}

void ProtocolParser::yield_to_client() const {
    (*caller_)();
}

void ProtocolParser::throw_if_poisoned(PDU const& top) const {
    if (top.size == 0 && top.pos == 0) {
        throw EStopIteration();
    }
}

void ProtocolParser::parse_next(PDU pdu) {
    buffers_.push(pdu);
    yield_to_worker();
}

Byte ProtocolParser::get() {
    while(true) {
        if (buffers_.empty()) {
            yield_to_client();
        }
        auto& top = buffers_.front();
        if (top.pos == top.size) {
            yield_to_client();
            continue;
        }
        throw_if_poisoned(top);
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos++];
        }
        buffers_.pop();
    }
    throw StreamError("unexpected end of stream");
}

Byte ProtocolParser::pick() const {
    while(true) {
        if (buffers_.empty()) {
            yield_to_client();
        }
        auto& top = buffers_.front();
        if (top.pos == top.size) {
            yield_to_client();
            continue;
        }
        throw_if_poisoned(top);
        if (top.pos < top.size) {
            auto buf = top.buffer.get();
            return buf[top.pos];
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
        if (top.pos < top.size) {
            size_t sz = top.size - top.pos;
            size_t bytes_to_copy = std::min(sz, buffer_len);
            memcpy(buffer, top.buffer.get(), sz);
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
    buffers_.push(POISON_);
}

}
