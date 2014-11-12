#include "compression.h"

namespace Akumuli {

aku_Status create_chunk( ChunkDesc          *out_desc
                       , aku_TimeStamp      *ts_begin
                       , aku_TimeStamp      *ts_end
                       , ChunkWriter        *writer
                       , const ChunkHeader&  data
                       , uint32_t            checksum)
{
    // NOTE: it is possible to avoid copying and write directly to page
    // instead of temporary byte vectors
    ByteVector timestamps;
    ByteVector paramids;
    ByteVector offsets;
    ByteVector lengths;

    DeltaRLETSWriter timestamp_stream(timestamps);
    Base128IdWriter paramid_stream(paramids);
    DeltaRLEOffWriter offset_stream(offsets);
    RLELenWriter length_stream(lengths);

    for (auto i = 0ul; i < data.timestamps.size(); i++) {
        timestamp_stream.put(data.timestamps.at(i));
        paramid_stream.put(data.paramids.at(i));
        offset_stream.put(data.offsets.at(i));
        length_stream.put(data.lengths.at(i));
    }
    timestamp_stream.close();
    paramid_stream.close();
    offset_stream.close();
    length_stream.close();

    uint32_t size_estimate =
            static_cast<uint32_t>( timestamp_stream.size()
                                 + paramid_stream.size()
                                 + offset_stream.size()
                                 + length_stream.size());

    aku_Status status = AKU_SUCCESS;

    switch(status) {
    case AKU_SUCCESS:
        status = writer->add_chunk(offset_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= offset_stream.size();
        status = writer->add_chunk(length_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= offset_stream.size();
        status = writer->add_chunk(paramid_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= paramid_stream.size();
        status = writer->add_chunk(timestamp_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        *out_desc = {
            static_cast<uint32_t>(data.lengths.size()),
            begin,
            end,
            checksum
        };
        *ts_begin = data.timestamps.front();
        *ts_end   = data.timestamps.back();
    }
    return status;
}

}
