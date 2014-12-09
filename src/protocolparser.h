#pragma once

// Using old-style boost.coroutines
#define BOOST_COROUTINES_BIDIRECT
#include <boost/coroutine/all.hpp>
#include <memory>
#include <cstdint>

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
public:
    bool is_done();
    void parse_next(PDU pdu);
};

}  // namespace

