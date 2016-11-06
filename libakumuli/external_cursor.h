#pragma once

#include "akumuli_def.h"

namespace Akumuli {

/** Data retreival interface that can be used by
 *  code that reads data from akumuli.
 */
struct ExternalCursor {

    /** New read interface for variably sized samples.
     * @param buffer is an array of aku_Sample structs
     * @param item_size defines size of each struct 0 - size = sizeof(aku_Sample)
     * @param buffer_size defines size of the buffer in bytes (should be a multiple of item_size)
     * @return number of overwritten bytes in `buffer`
     */
    virtual u32 read(void* buffer, u32 buffer_size) = 0;

    //! Check is everything done
    virtual bool is_done() const = 0;

    //! Check is error occured and (optionally) get the error code
    virtual bool is_error(aku_Status* out_error_code_or_null = nullptr) const = 0;

    //! Finalizer
    virtual void close() = 0;

    virtual ~ExternalCursor() = default;
};

}  // namespace
