#pragma once
#include <stdint.h>
#include <stddef.h>
// CRC32C calculation
// This implementation is based on Mark Adler's implementation here -
//      http://stackoverflow.com/questions/17645167/implementing-sse-4-2s-crc32c-in-software

namespace Akumuli {

typedef uint32_t (*crc32c_impl_t)(uint32_t crc, const void *buf, size_t len);

enum class CRC32C_hint {
    DETECT,
    FORCE_SW,
    FORCE_HW,
};

//! Return crc32c implementation.
crc32c_impl_t chose_crc32c_implementation(CRC32C_hint hint=CRC32C_hint::DETECT);

}

