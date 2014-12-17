#include "protocolparser.h"
#include "resp.h"

namespace Akumuli {

void ProtocolParser::worker(Caller& yield) {
    while(true) {
        if (input_.empty()) {
            yield();
            continue;
        }
        PDURef buf = std::move(input_.front());

        // TODO: concat with prev stream
    }
}

bool ProtocolParser::is_done() {
    throw std::runtime_error("Not implemented");
}

void ProtocolParser::parse_next(PDU pdu) {
    throw std::runtime_error("Not implemented");
}

}
