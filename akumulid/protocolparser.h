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
#include <boost/coroutine/asymmetric_coroutine.hpp>
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include "logger.h"
#include "protocol_consumer.h"
#include "resp.h"
#include "stream.h"

namespace Akumuli {

/** Protocol Data Unit */
struct PDU {
    std::shared_ptr<const Byte>
           buffer;  //< Pointer to buffer (buffer can be referenced by several PDU)
    size_t size;    //< Size of the buffer
    size_t pos;     //< Position in the buffer
};

struct ProtocolParserError : StreamError {
    ProtocolParserError(std::string line, int pos);
};

// TODO: On clang, gives "error: destructor called on non-final boost::coroutines::detail::push_coroutine_object<...>
// that has virtual functions but non-virtual destructor [-Werror,-Wdelete-non-virtual-dtor]"
// on obj->~obj_t();
// at /include/boost/coroutine/detail/push_coroutine_object.hpp:81:9
typedef typename boost::coroutines::asymmetric_coroutine<void()>::push_type Coroutine;
typedef typename boost::coroutines::asymmetric_coroutine<void()>::pull_type Caller;


//! Stop iteration exception
struct EStopIteration {};


class ProtocolParser : ByteStreamReader {
    mutable std::shared_ptr<Coroutine> coroutine_;
    mutable Caller*                    caller_;
    mutable std::queue<PDU>            buffers_;
    static const PDU                   POISON_;  //< This object marks end of the stream
    bool                               done_;
    std::shared_ptr<ProtocolConsumer>  consumer_;
    Logger                             logger_;

    void worker(Caller& yield);
    void set_caller(Caller& caller);
    //! Yield control to worker
    void yield_to_worker();
    //! Yield control to external code
    void yield_to_client() const;
    //! Throw exception if poisoned
    void throw_if_poisoned(PDU const& top) const;
    //! Generate error message
    std::tuple<std::string, size_t> get_error_from_pdu(PDU const& pdu) const;

public:
    ProtocolParser(std::shared_ptr<ProtocolConsumer> consumer);
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
};


}  // namespace
