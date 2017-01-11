/**
 * Copyright (c) 2014 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// Using old-style boost.coroutines
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>
#include <cassert>  // TODO: move to cpp

#include "logger.h"
#include "resp.h"
#include "stream.h"

namespace Akumuli {

/** Protocol Data Unit */
struct PDU {
    std::shared_ptr<const Byte>
        buffer;  //< Pointer to buffer (buffer can be referenced by several PDU)
    u32 size;    //< Size of the buffer
    u32 pos;     //< Position in the buffer
    u32 cons;    //< Consumed part
};

struct ProtocolParserError : StreamError {
    ProtocolParserError(std::string line, size_t pos);
};

struct DatabaseError : std::exception {
    aku_Status status;
    DatabaseError(aku_Status status);
    virtual const char* what() const noexcept;
};

//! Stop iteration exception
struct EStopIteration {};

//! Fwd
struct DbSession;


/** ChunkedWriter used by servers to acquire buffers.
  * Server should follow the protocol:
  * - pull buffer
  * - fill buffer with data
  * - push buffer
  * - pull next buffer
  * - etc...
  */
struct ChunkedWriter {
    typedef Byte* BufferT;
    virtual BufferT pull() = 0;
    virtual void push(BufferT buffer, u32 size) = 0;
};


/** This class should be used in conjunction with tcp-server class.
 * It allocates buffers for server and makes them available to parser.
 */
class ReadBuffer : public ByteStreamReader, public ChunkedWriter {
    enum {
        N_BUF = 8,
    };
    const size_t BUFFER_SIZE;
    std::vector<Byte> buffer_;
    mutable u32 rpos_;   // Current read position
    mutable u32 wpos_;   // Current write position
    mutable u32 cons_;   // Consumed part of the buffer
    int buffers_allocated_; // Buffer counter (only one allocated buffer is allowed)

public:
    ReadBuffer(const size_t buffer_size)
        : BUFFER_SIZE(buffer_size)
        , buffer_(buffer_size*N_BUF, 0)
        , rpos_(0)
        , wpos_(0)
        , cons_(0)
        , buffers_allocated_(0)
    {
    }

    // ByteStreamReader interface
public:
    virtual Byte get() override {
        if (rpos_ == wpos_) {
            auto ctx = get_error_context("unexpected end of stream");
            BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        return buffer_[rpos_++];
    }
    virtual Byte pick() const override {
        if (rpos_ == wpos_) {
            auto ctx = get_error_context("unexpected end of stream");
            BOOST_THROW_EXCEPTION(ProtocolParserError(std::get<0>(ctx), std::get<1>(ctx)));
        }
        return buffer_[rpos_];
    }
    virtual bool is_eof() override {
        return rpos_ == wpos_;
    }
    virtual int read(Byte *buffer, size_t buffer_len) override {
        assert(buffer_len < 0x100000000ul);
        u32 to_read = wpos_ - rpos_;
        to_read = std::min(static_cast<u32>(buffer_len), to_read);
        std::copy(buffer_.begin() + rpos_, buffer_.begin() + rpos_ + to_read, buffer);
        rpos_ += to_read;
        return static_cast<int>(to_read);
    }
    virtual int read_line(Byte* buffer, size_t quota) override {
        assert(quota < 0x100000000ul);
        u32 available = wpos_ - rpos_;
        auto to_read = std::min(static_cast<u32>(quota), available);
        for (u32 i = 0; i < to_read; i++) {
            Byte c = buffer_[rpos_ + i];
            buffer[i] = c;
            if (c == '\n') {
                // Stop iteration
                u32 bytes_copied = i + 1;
                rpos_ += bytes_copied;
                return static_cast<int>(bytes_copied);
            }
        }
        // No end of line found
        return -1*static_cast<int>(to_read);
    }
    virtual void close() override {
    }
    virtual std::tuple<std::string, size_t> get_error_context(const char *error_message) const override {
        return std::make_tuple(std::string("Can't generate error, not implemented"), 0u);
    }
    virtual void consume() {
        assert(buffers_allocated_ == 0);  // Invariant check: buffer can be invalidated!
        cons_ = rpos_;
    }
    virtual void discard() {
        assert(buffers_allocated_ == 0);  // Invariant check: buffer can be invalidated!
        rpos_ = cons_;
    }

    // BufferAllocator interface
public:

// TODO: move implementation to cpp

    /** Return pointer to buffer. Size of the buffer is guaranteed to be at least
      * BUFFER_SIZE bytes.
      */
    virtual BufferT pull() override {
        assert(buffers_allocated_ == 0);  // Invariant check: buffer will be invalidated after vector.resize!
        buffers_allocated_++;

        u32 sz = static_cast<u32>(buffer_.size()) - wpos_;  // because previous push can bring partially filled buffer
        if (sz < BUFFER_SIZE) {
            if ((cons_ + sz) > BUFFER_SIZE) {
                // Problem can be solved by rotating the buffer and asjusting wpos_, rpos_ and cons_
                std::copy(buffer_.begin() + cons_, buffer_.end(), buffer_.begin());
                wpos_ -= cons_;
                rpos_ -= cons_;
                cons_ = 0;
            } else {
                // Double the size of the buffer
                buffer_.resize(buffer_.size() * 2);
            }
        }
        Byte* ptr = buffer_.data() + wpos_;
        return ptr;
    }

    virtual void push(BufferT, u32 size) override {
        assert(buffers_allocated_ == 1);
        buffers_allocated_--;
        wpos_ += size;
    }
};


class ProtocolParser {
    ReadBuffer                         rdbuf_;
    bool                               done_;
    std::shared_ptr<DbSession>         consumer_;
    Logger                             logger_;

    //! Process frames from queue
    void worker();
    //! Generate error message
    std::tuple<std::string, size_t> get_error_from_pdu(PDU const& pdu) const;

    void backlog_top() const;
public:
    enum {
        RDBUF_SIZE = 0x1000,  // 4KB
    };
    ProtocolParser(std::shared_ptr<DbSession> consumer);
    void start();
    void parse_next(Byte *buffer, u32 sz);
    void close();
    Byte* get_next_buffer();
};


}  // namespace
