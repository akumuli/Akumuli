#pragma once

#include <iostream>
#include <cstdint>


typedef uint64_t u64;
typedef uint32_t u32;
typedef int64_t  i64;
typedef int32_t  i32;

namespace Akumuli {

/** The messages supported by the chain-replication protocol implementation:
 *  - Header
 *  - Data payload
 *  - Set tail
 *  - Dictionary update
 *
 *  Every message should define stream operators and wireLength method.
 */

enum class MessageType : char
{
    FLOAT = 1,
    EVENT = 2,
    DICT  = 4,
    TAIL  = 8,
};

struct Header {
    char          hop;
    MessageType  type;
    u32          size;
    u32 wireLength() const;
};

struct DataPayload {
    u32            id;
    u64     timestamp;
    double      value;
    u32 wireLength() const;
};

struct EventPayload {
    u32            id;
    u64     timestamp;
    std::string value;
    u32 wireLength() const;
};

struct DictionaryUpdate {
    u32            id;
    std::string sname;
    u32 wireLength() const;
};

struct SetTail {
    std::string  host;
    u32          port;
    u32 wireLength() const;
};

struct Reply {
    u64 seq;
    u32 status;
    std::string error_message;
    u32 wireLength() const;
};

std::ostream& operator << (std::ostream& s, const Header& payload);
std::ostream& operator << (std::ostream& s, const DataPayload& payload);
std::ostream& operator << (std::ostream& s, const EventPayload& payload);
std::ostream& operator << (std::ostream& s, const DictionaryUpdate& payload);
std::ostream& operator << (std::ostream& s, const SetTail& payload);
std::ostream& operator << (std::ostream& s, const Reply& payload);

std::istream& operator >> (std::istream& s, Header& payload);
std::istream& operator >> (std::istream& s, DataPayload& payload);
std::istream& operator >> (std::istream& s, EventPayload& payload);
std::istream& operator >> (std::istream& s, DictionaryUpdate& payload);
std::istream& operator >> (std::istream& s, SetTail& payload);
std::istream& operator >> (std::istream& s, Reply& payload);

}
