#pragma once
#include <akumuli.h>
#include <unordered_map>

namespace Akumuli {

/** Query processor.
  * Should be built from textual representation (json at first).
  * Should be used by both sequencer and page to match parameters
  * and group them together.
  */
struct QueryProcessor {
    aku_TimeStamp                          lowerbound;
    aku_TimeStamp                          upperbound;
    int                                     direction;
    std::unordered_map<uint64_t, uint64_t>  idmapping;
};

}
