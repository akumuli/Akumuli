#include "binaryprotocol.h"
#include <vector>
#include <cstring>

namespace Akumuli {

std::ostream& operator << (std::ostream& s, const Header& payload) {
    char buffer[sizeof(payload)];
    memcpy(buffer, &payload, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    return s;
}

std::istream& operator >> (std::istream& s, Header& payload) {
    char buffer[sizeof(payload)];
    s.read(buffer, sizeof(payload));
    memcpy(&payload, buffer, sizeof(buffer));
    return s;
}

u32 Header::wireLength() const {
    return sizeof(Header);
}

std::ostream& operator << (std::ostream& s, const DataPayload& payload) {
    char buffer[sizeof(payload)];
    memcpy(buffer, &payload, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    return s;
}

std::istream& operator >> (std::istream& s, DataPayload& payload) {
    char buffer[sizeof(payload)];
    s.read(buffer, sizeof(payload));
    memcpy(&payload, buffer, sizeof(buffer));
    return s;
}

u32 DataPayload::wireLength() const {
    return sizeof(DataPayload);
}

std::ostream& operator << (std::ostream& s, const EventPayload& payload) {
    char buffer[sizeof(u64)];
    memcpy(buffer, &payload.id, sizeof(u32));
    s.write(buffer, sizeof(u32));
    memcpy(buffer, &payload.timestamp, sizeof(u64));
    s.write(buffer, sizeof(u64));
    u32 len = static_cast<u32>(payload.value.size());
    memcpy(buffer, &len, sizeof(len));
    s.write(buffer, sizeof(len));
    s.write(payload.value.data(), len);
    return s;
}

std::istream& operator >> (std::istream& s, EventPayload& payload) {
    char buffer[sizeof(u64)];
    s.read(buffer, sizeof(u32));
    memcpy(&payload.id, buffer, sizeof(u32));
    s.read(buffer, sizeof(u64));
    memcpy(&payload.timestamp, buffer, sizeof(u64));
    u32 len;
    s.read(buffer, sizeof(u32));
    memcpy(&len, buffer, sizeof(u32));
    std::vector<char> event(len);
    s.read(event.data(), len);
    payload.value.assign(event.begin(), event.end());
    return s;
}

u32 EventPayload::wireLength() const {
    return sizeof(u32)*2 + sizeof(u64) + value.size();
}


std::ostream& operator << (std::ostream& s, const DictionaryUpdate& payload) {
    char buffer[sizeof(payload.id)];
    memcpy(buffer, &payload.id, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    u32 slen = static_cast<u32>(payload.sname.size());
    memcpy(buffer, &slen, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    s.write(payload.sname.data(), slen);
    return s;
}

std::istream& operator >> (std::istream& s, DictionaryUpdate& payload) {
    char buffer[sizeof(u32)];
    s.read(buffer, sizeof(u32));
    memcpy(&payload.id, buffer, sizeof(u32));
    u32 slen;
    s.read(buffer, sizeof(u32));
    memcpy(&slen, buffer, sizeof(u32));
    std::vector<char> name(slen);
    s.read(name.data(), slen);
    payload.sname.assign(name.begin(), name.end());
    return s;
}

u32 DictionaryUpdate::wireLength() const {
    return sizeof(u32)*2 + sname.size();
}

std::ostream& operator << (std::ostream& s, const SetTail& payload) {
    char buffer[sizeof(payload.port)];
    memcpy(buffer, &payload.port, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    u32 hlen = static_cast<u32>(payload.host.size());
    memcpy(buffer, &hlen, sizeof(buffer));
    s.write(buffer, sizeof(buffer));
    s.write(payload.host.data(), hlen);
    return s;
}

std::istream& operator >> (std::istream& s, SetTail& payload) {
    char buffer[sizeof(u32)];
    s.read(buffer, sizeof(u32));
    memcpy(&payload.port, buffer, sizeof(u32));
    u32 hlen;
    s.read(buffer, sizeof(u32));
    memcpy(&hlen, buffer, sizeof(u32));
    std::vector<char> host(hlen);
    s.read(host.data(), hlen);
    payload.host.assign(host.begin(), host.end());
    return s;
}

u32 SetTail::wireLength() const {
    return sizeof(u32)*2 + host.size();
}

std::ostream& operator << (std::ostream& s, const Reply& payload) {
    char buffer[sizeof(payload.seq)];
    memcpy(buffer, &payload.seq, sizeof(payload.seq));
    s.write(buffer, sizeof(buffer));
    memcpy(buffer, &payload.status, sizeof(payload.status));
    s.write(buffer, sizeof(payload.status));
    u32 len = static_cast<u32>(payload.error_message.size());
    memcpy(buffer, &len, sizeof(len));
    s.write(buffer, sizeof(len));
    s.write(payload.error_message.data(), len);
    return s;
}

std::istream& operator >> (std::istream& s, Reply& payload) {
    char buffer[sizeof(payload.seq)];
    s.read(buffer, sizeof(payload.seq));
    memcpy(&payload.seq, buffer, sizeof(payload.seq));
    s.read(buffer, sizeof(payload.status));
    memcpy(&payload.status, buffer, sizeof(payload.status));
    u32 len;
    s.read(buffer, sizeof(u32));
    memcpy(&len, buffer, sizeof(u32));
    std::vector<char> errormsg(len);
    s.read(errormsg.data(), len);
    payload.error_message.assign(errormsg.begin(), errormsg.end());
    return s;
}

u32 Reply::wireLength() const {
    return sizeof(u64) + sizeof(u32)*2 + error_message.size();
}

}
