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

namespace Akumuli {

/** Protocol consumer. All decoded data goes here.
  * Abstract class.
  */
struct ProtocolConsumer {
    ~ProtocolConsumer() {}
};

/** Protocol Data Unit */
struct PDU {
    std::shared_ptr<void*> buffer;
    size_t buffer_size;
};

typedef boost::coroutines::coroutine< void(ProtocolConsumer*) > Coroutine;
typedef typename Coroutine::caller_type Caller;

class ProtocolParser
{
    typedef std::unique_ptr<PDU> PDURef;
    std::shared_ptr<Coroutine> coroutine_;
    std::queue<PDURef> input_;
    bool stop_;

    void worker(Caller &yield);
public:
    ProtocolParser();
    bool is_done();
    void parse_next(PDU pdu);
};

}  // namespace

