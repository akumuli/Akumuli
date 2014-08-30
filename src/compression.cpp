#include "compression.h"

namespace Akumuli {

    Base128Int::Base128Int(uint64_t val) : value_(val) {
    }

    Base128Int::Base128Int() : value_(0ul) {
    }

    Base128Int::operator uint64_t() const {
        return value_;
    }
}
