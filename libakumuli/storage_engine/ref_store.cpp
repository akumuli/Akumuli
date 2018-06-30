#include "ref_store.h"

namespace Akumuli {
namespace StorageEngine {

u8* SubtreeRefCompressor::encode_subtree_ref(u8* dest, size_t dest_size, const SubtreeRef& ref) {
    auto begin = dest;
    auto end   = dest + dest_size;

    auto put_u64 = [&](u64 val) {
        Base128Int<u64> enc(val);
        return enc.put(begin, end);
    };

    auto put_double = [&](double x) {
        // Put without compression
        union {
            double x;
            u8 bits[8];
        } value;
        value.x = x;
        if (end - begin > 8) {
            memcpy(begin, value.bits, 8);
            return begin + 8;
        }
        return begin;
    };

    // This macro calls the 'exp' expression, checks output address
    // and adjusts 'begin' or returns.
    #define ENCODE_NEXT(exp) { \
        auto outp = (exp); \
        if (outp == begin) { \
            return dest; \
        } else { \
            begin = outp; \
        }\
    }\

    /*
     * Format description.
     *
     * Each record is var-length. It starts with 1-byte header
     * that contain length of the record (so the record can be
     * skipped without decoding). The header is followed by the
     * `SubtreeRef::level` value which is encoded using the LEB128
     * algorithm. Since the value is less then 10 it takes only
     * 1-byte to store it. The value is located near the header
     * of the record because it's the only value needed to filter
     * records. Other fields of the `SubtreeRef` follows. The
     * integers are encoded using LEB128 and double values are
     * stored as-is. Version and id are not stored since it's the
     * same for all records.
     */

    u8* length = begin++;

    ENCODE_NEXT(put_u64(ref.level));

    ENCODE_NEXT(put_u64(ref.count));

    ENCODE_NEXT(put_u64(ref.begin));

    auto dend = ref.end - ref.begin;
    ENCODE_NEXT(put_u64(dend));

    auto dmin = ref.min_time - ref.begin;
    ENCODE_NEXT(put_u64(dmin));

    auto dmax = ref.max_time - ref.begin;
    ENCODE_NEXT(put_u64(dmax));

    ENCODE_NEXT(put_u64(ref.addr));

    ENCODE_NEXT(put_double(ref.min));

    ENCODE_NEXT(put_double(ref.max));

    ENCODE_NEXT(put_double(ref.sum));

    ENCODE_NEXT(put_double(ref.first));

    ENCODE_NEXT(put_double(ref.last));

    ENCODE_NEXT(put_u64(static_cast<u64>(ref.type)));

    // If type is a 'leaf' then the payload_size will contain a
    // size in bytes (close but less then 4096). Otherwise it
    // will contain a number of elements in the innter node that
    // is always less than 32.
    if (ref.type == NBTreeBlockType::INNER) {
        ENCODE_NEXT(put_u64(ref.payload_size));
    } else {
        ENCODE_NEXT(put_u64(AKU_BLOCK_SIZE - ref.payload_size));
    }

    ENCODE_NEXT(put_u64(ref.fanout_index));

    ENCODE_NEXT(put_u64(ref.checksum));

    #undef ENCODE_NEXT

    *length = static_cast<u8>(begin - dest);

    return begin;
}

const u8* SubtreeRefCompressor::decode_subtree_ref(const u8* source, size_t source_size, SubtreeRef* ref) {
    const u8* begin = source;
    const u8* end = source + source_size;

    auto get_u64 = [&]() {
        Base128Int<u64> enc;
        auto ret = enc.get(begin, end);
        auto value = static_cast<u64>(enc);
        return std::make_tuple(ret, value);
    };

    auto get_u16 = [&]() {
        Base128Int<u16> enc;
        auto ret = enc.get(begin, end);
        auto value = static_cast<u16>(enc);
        return std::make_tuple(ret, value);
    };

    auto get_double = [&]() {
        union {
            double x;
            u8 bits[8];
        } value;
        if (end - begin > 8) {
            memcpy(value.bits, begin, 8);
            return std::make_tuple(begin + 8, value.x);
        }
        return std::make_tuple(begin, 0.0);
    };

    // This macro is used to extract the value using the expression 'exp',
    // move the result to 'dest' on success or return on error. The error
    // will occur if the buffer is incomplete.
#   define DECODE_NEXT(dest, exp) {\
        auto retval = (exp);\
        if (std::get<0>(retval) == begin) {\
            return source;\
        } else {\
            begin = std::get<0>(retval);\
            dest  = static_cast<decltype(dest)>(std::get<1>(retval));\
        }\
    }

    u8 length = *begin++;

    DECODE_NEXT(ref->level, get_u16());

    DECODE_NEXT(ref->count, get_u64());

    DECODE_NEXT(ref->begin, get_u64());

    u64 dend = 0;
    DECODE_NEXT(dend, get_u64());
    ref->end = ref->begin + dend;

    u64 dmin = 0;
    DECODE_NEXT(dmin, get_u64());
    ref->min_time = ref->begin + dmin;

    u64 dmax = 0;
    DECODE_NEXT(dmax, get_u64());
    ref->max_time = ref->begin + dmax;

    DECODE_NEXT(ref->addr, get_u64());

    DECODE_NEXT(ref->min, get_double());

    DECODE_NEXT(ref->max, get_double());

    DECODE_NEXT(ref->sum, get_double());

    DECODE_NEXT(ref->first, get_double());

    DECODE_NEXT(ref->last, get_double());

    DECODE_NEXT(ref->type, get_u16());

    DECODE_NEXT(ref->payload_size, get_u16());
    if (ref->type != NBTreeBlockType::INNER) {
        ref->payload_size = AKU_BLOCK_SIZE - ref->payload_size;
    }

    DECODE_NEXT(ref->fanout_index, get_u16());

    DECODE_NEXT(ref->checksum, get_u64());

    #undef DECODE_NEXT

    // Verify read
    if (length != static_cast<u8>(begin - source)) {
        return source;
    }

    return begin;
}

//! Count space occupied by the records with level other than provided
static size_t count_others(const u8* source, size_t source_size, u16 level)
{
    auto begin = source;
    auto end = begin + source_size;
    size_t size = 0;
    while((begin + 1) < end) {
        u8 length = *begin;
        Base128Int<u16> level;
        auto p = level.get(begin + 1, end);
        if (p == begin || (begin + length) >= end) {
            break;
        }
        if (level != level) {
            size += length;
        }
        begin += length;
    }
    return size;
}

void SubtreeRefCompressor::filter(const u8* source,
                                  size_t source_size,
                                  u16 level2remove,
                                  std::vector<u8> *out)
{
    size_t out_size = count_others(source, source_size, level2remove);
    out->reserve(out_size);
    auto begin = source;
    auto end = begin + source_size;
    while((begin + 1) < end) {
        u8 length = *begin;
        // Tap into message to see if it should be copied
        Base128Int<u16> level;
        auto p = level.get(begin + 1, end);
        if (p == begin) {
            break;
        }
        if ((begin + length) > end) {
            AKU_PANIC("Memory corrupted");
        }
        if (level != level2remove) {
            std::copy(begin, begin + length, std::back_inserter(*out));
        }
        begin += length;
    }
}


CompressedRefStorage::CompressedRefStorage(aku_ParamId id, u16 version)
    : id_(id)
    , version_(version)
{
}

}
}
