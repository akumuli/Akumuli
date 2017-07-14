#pragma once

#include "operator.h"
#include "merge.h"


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

    /** Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);
};


/** Operator that can be used to join several series.
  * This materializer is based on merge-join but returns tuples ordered by time
  * instead of individual values.
  * Tuple can contain up to 58 elements.
  */
class JoinMaterializer2 : public ColumnMaterializer {

    std::unique_ptr<ColumnMaterializer> merge_;         //< underlying merge-iterator
    std::vector<aku_ParamId>            orig_ids_;      //< array of original ids
    aku_ParamId                         id_;            //< id of the resulting time-series
    aku_Timestamp                       curr_;          //< timestamp of the currently processed sample
    std::vector<u8>                     buffer_;        //< the read buffer
    u32                                 buffer_size_;   //< read buffer size (capacity is defined by the vector size)
    u32                                 buffer_pos_;    //< position in the read buffer
    const u32                           max_ssize_;     //< element size (in bytes)

public:

    /**
     * @brief JoinMaterializer2 c-tor
     * @param ids is a original ids of the series
     * @param iters is an array of scan operators
     * @param id is an id of the resulting series
     */
    JoinMaterializer2(std::vector<aku_ParamId> &&ids,
                      std::vector<std::unique_ptr<RealValuedOperator>>&& iters,
                      aku_ParamId id);

    /**
      * @brief Read materialized value into buffer
      * Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size);

private:
    aku_Status fill_buffer();
};

struct JoinConcatMaterializer : ColumnMaterializer {
    std::vector<std::unique_ptr<ColumnMaterializer>> iters_;
    size_t ix_;

    JoinConcatMaterializer(std::vector<std::unique_ptr<ColumnMaterializer>>&& iters)
        : iters_(std::move(iters))
        , ix_(0)
    {
    }

    /** Read values to buffer. Values is aku_Sample with variable sized payload.
      * Format: float64 contains bitmap, data contains array of nonempty values (whether a
      * value is empty or not is defined by bitmap)
      * @param dest is a pointer to recieving buffer
      * @param size is a size of the recieving buffer
      * @return status and output size (in bytes)
      */
    std::tuple<aku_Status, size_t> read(u8 *dest, size_t size) {
        while(true) {
            if (ix_ >= iters_.size()) {
                return std::make_tuple(AKU_ENO_DATA, 0);
            }
            aku_Status status;
            size_t outsz;
            std::tie(status, outsz) = iters_.at(ix_)->read(dest, size);
            if (status == AKU_ENO_DATA) {
                ix_++;
                if (outsz != 0) {
                    return std::make_tuple(ix_ != iters_.size() ? AKU_SUCCESS : AKU_ENO_DATA, outsz);
                }
            } else {
                return std::make_tuple(status, outsz);
            }
        }
    }
};

}
}
