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
        // This parameter defines initial buffer size as a number of BUFFER_SIZE regions.
        // Increasing this parameter will increase memory requirements. Decreasing this parameter
        // will increase amount of copying.
        N_BUF = 4,
    };
    const size_t BUFFER_SIZE;
    std::vector<Byte> buffer_;
    mutable u32 rpos_;   // Current read position
    mutable u32 wpos_;   // Current write position
    mutable u32 cons_;   // Consumed part of the buffer
    int buffers_allocated_; // Buffer counter (only one allocated buffer is allowed)

public:
    ReadBuffer(const size_t buffer_size);

    // ByteStreamReader interface
public:
    virtual Byte get() override;
    virtual Byte pick() const override;
    virtual bool is_eof() override;
    virtual int read(Byte *buffer, size_t buffer_len) override;
    virtual int read_line(Byte* buffer, size_t quota) override;
    virtual void close() override;
    virtual std::tuple<std::string, size_t> get_error_context(const char *error_message) const override;
    virtual void consume();
    virtual void discard();

    // BufferAllocator interface
public:
    /** Get pointer to buffer. Size of the buffer is guaranteed to be at least
      * BUFFER_SIZE bytes.
      */
    virtual BufferT pull() override;
    /** Return previously acquired buffer.
      */
    virtual void push(BufferT, u32 size) override;
};


class ProtocolParser {
    bool                               done_;
    ReadBuffer                         rdbuf_;
    std::shared_ptr<DbSession>         consumer_;
    Logger                             logger_;

    //! Process frames from queue
    void worker();
    //! Generate error message
    std::tuple<std::string, size_t> get_error_from_pdu(PDU const& pdu) const;

    bool parse_timestamp(RESPStream& stream, aku_Sample& sample);
    bool parse_values(RESPStream& stream, double* values, int nvalues);
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
