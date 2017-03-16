#pragma once

#include "operator.h"


namespace Akumuli {
namespace StorageEngine {


/** Operator that can be used to align several trees together
  */
struct JoinMaterializer : ColumnMaterializer {

    std::vector<std::unique_ptr<RealValuedOperator>> iters_;
    aku_ParamId id_;
    static const size_t BUFFER_SIZE = 4096;
    static const size_t MAX_TUPLE_SIZE = 64;
    std::vector<std::vector<std::pair<aku_Timestamp, double>>> buffers_;
    u32 buffer_pos_;
    u32 buffer_size_;

    JoinMaterializer(std::vector<std::unique_ptr<RealValuedOperator>>&& iters, aku_ParamId id);

    aku_Status fill_buffers();

    /** Get pointer to buffer and return pointer to sample and tuple data */
    static std::tuple<aku_Sample*, double*> cast(u8* dest);

    /** Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};


}
}
