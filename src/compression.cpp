#include "compression.h"

namespace Akumuli {

aku_Status CompressionUtil::encode_chunk( uint32_t           *n_elements
                                        , aku_TimeStamp      *ts_begin
                                        , aku_TimeStamp      *ts_end
                                        , ChunkWriter        *writer
                                        , const ChunkHeader&  data)
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
                                 + length_stream.size()
                                 + data.values.size()*sizeof(double)
                                 + sizeof(uint32_t)
                                 );

    aku_Status status = AKU_SUCCESS;

    switch(status) {
    case AKU_SUCCESS:
        // Doubles
        uint32_t doubles_size = static_cast<uint32_t>(data.values.size());
        if (doubles_size != 0) {
            size_estimate -= data.values.size() * sizeof(double);
            const aku_MemRange doubles_mrange = {
                const_cast<void*>((const void*)data.values.data()),
                doubles_size*8,
            };
            status = writer->add_chunk(doubles_mrange, size_estimate);
            if (status != AKU_SUCCESS) {
                break;
            }
        }
        aku_MemRange doubles_size_mrange = {
            &doubles_size,
            sizeof(doubles_size),
        };
        // Doubles size
        status = writer->add_chunk(doubles_size_mrange, size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= sizeof(doubles_size);
        // Offsets
        status = writer->add_chunk(offset_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= offset_stream.size();
        // Lengths
        status = writer->add_chunk(length_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= offset_stream.size();
        // Param-Ids
        status = writer->add_chunk(paramid_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= paramid_stream.size();
        // Timestamps
        status = writer->add_chunk(timestamp_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        *n_elements = static_cast<uint32_t>(data.lengths.size());
        *ts_begin = data.timestamps.front();
        *ts_end   = data.timestamps.back();
    }
    return status;
}

int CompressionUtil::decode_chunk( ChunkHeader *header
                                 , const unsigned char **pbegin
                                 , const unsigned char *pend
                                 , int stage
                                 , int steps
                                 , uint32_t probe_length)
{
    if (steps <= 0) {
        return 0;
    }
    switch(stage) {
    case 0: {
        // read timestamps
        DeltaRLETSReader tst_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->timestamps.push_back(tst_reader.next());
        }
        *pbegin = tst_reader.pos();
        if (--steps == 0) {
            return 1;
        }
    }
    case 1: {
        // read paramids
        Base128IdReader pid_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->paramids.push_back(pid_reader.next());
        }
        *pbegin = pid_reader.pos();
        if (--steps == 0) {
            return 2;
        }
    }
    case 2: {
        // read lengths
        RLELenReader len_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->lengths.push_back(len_reader.next());
        }
        *pbegin = len_reader.pos();
        if (--steps == 0) {
            return 3;
        }
    }
    case 3: {
        // read offsets
        DeltaRLEOffReader off_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->offsets.push_back(off_reader.next());
        }
        *pbegin = off_reader.pos();
        if (--steps == 0) {
            return 4;
        }
    }
    case 4: {
        // read doubles
        uint32_t doubles_size = *reinterpret_cast<const uint32_t*>(*pbegin);
        *pbegin += sizeof(uint32_t);
        const double *pdouble = reinterpret_cast<const double*>(*pbegin);
        for (auto i = doubles_size; i --> 0;) {
            header->values.push_back(*pdouble++);
        }
        *pbegin = reinterpret_cast<const unsigned char*>(pdouble);
        if (--steps == 0) {
            return 5;
        }
    }
    default:
        break;
    }
    return -1;
}

}
