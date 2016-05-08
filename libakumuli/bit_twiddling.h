#pragma once

namespace Akumuli {

/** Static struct to organaize differnet bit-hacks.
  */
struct BitHacks {

    //! Return number of 1-bits in bitmap.
    static inline int count_bits(u32 bitmap) {
#if __GNUC__
        return __builtin_popcount(bitmap);
#else
        bitmap = bitmap - ((bitmap >> 1) & 0x55555555);
        bitmap = (bitmap & 0x33333333) + ((bitmap >> 2) & 0x33333333);
        return (((bitmap + (bitmap >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
#endif
    }
};
}
