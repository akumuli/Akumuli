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

class ProtocolParser : ByteStreamReader {
    mutable std::queue<PDU>            buffers_;
    mutable std::queue<PDU>            backlog_;
    bool                               done_;
    std::shared_ptr<DbSession>         consumer_;
    Logger                             logger_;

    //! Process frames from queue
    void worker();
    //! Generate error message
    std::tuple<std::string, size_t> get_error_from_pdu(PDU const& pdu) const;

    void backlog_top() const;
public:
    ProtocolParser(std::shared_ptr<DbSession> consumer);
    void start();
    void parse_next(PDU pdu);

    // ByteStreamReader interface
public:
    virtual Byte get();
    virtual Byte pick() const;
    virtual bool is_eof();
    virtual int read(Byte* buffer, size_t buffer_len);
    virtual void close();
    virtual std::tuple<std::string, size_t> get_error_context(const char* msg) const;
    virtual void consume();
    virtual void discard();
};


}  // namespace
