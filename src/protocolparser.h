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
#define BOOST_COROUTINES_BIDIRECT
#include <boost/coroutine/all.hpp>
#include <memory>
#include <cstdint>
#include <vector>
#include <queue>

#include "stream.h"
#include "akumuli.h"

namespace Akumuli {

/** Protocol consumer. All decoded data goes here.
  * Abstract class.
  */
struct ProtocolConsumer {

    ~ProtocolConsumer() {}

    virtual void write_double(aku_ParamId param, aku_TimeStamp ts, double data) = 0;
};

/** Protocol Data Unit */
struct PDU {
    std::shared_ptr<Byte> buffer;
    size_t size;
    size_t pos;
};

typedef std::runtime_error ProtocolParserError;

typedef boost::coroutines::coroutine< void() > Coroutine;
typedef typename Coroutine::caller_type Caller;


class ProtocolParser : ByteStreamReader {
    typedef std::unique_ptr<PDU> PDURef;
    std::shared_ptr<Coroutine> coroutine_;
    Caller *caller_;
    mutable std::queue<PDURef> buffers_;
    bool stop_;
    bool done_;
    std::unique_ptr<ProtocolConsumer> consumer_;

    void worker(Caller &yield);
    void set_caller(Caller& caller);
    //! Yield control to worker
    void yield_to_worker();
    //! Yield control to external code
    void yield_to_client();
public:
    ProtocolParser(std::unique_ptr<ProtocolConsumer> &&consumer);
    void start();
    void parse_next(PDURef&& pdu);

    // ByteStreamReader interface
public:
    virtual Byte get();
    virtual Byte pick() const;
    virtual bool is_eof();
    virtual int read(Byte *buffer, size_t buffer_len);
    virtual void close();
};


}  // namespace

