#include "compression.h"
#include <unordered_map>
#include <algorithm>

namespace Akumuli {

//! Stream that can be used to write data by 4-bits
struct HalfByteStreamWriter {
    Base128StreamWriter& stream;
    size_t write_pos;
    unsigned char tmp;

    HalfByteStreamWriter(Base128StreamWriter& stream, size_t numblocks=0u)
        : stream(stream)
        , write_pos(numblocks)
        , tmp(0)
    {
    }

    aku_Status add4bits(unsigned char value) {
        if (write_pos % 2 == 0) {
            tmp = value & 0xF;
        } else {
            tmp |= (value << 4);
            auto status = stream.put(tmp);
            if (status != AKU_SUCCESS) {
                return status;
            }
            tmp = 0;
        }
        write_pos++;
        return AKU_SUCCESS;
    }

    aku_Status close() {
        if (write_pos % 2 != 0) {
            auto status = stream.put(tmp);
            if (status != AKU_SUCCESS) {
                return status;
            }
        }
        return AKU_SUCCESS;
    }
};

//! Stream that can be used to write data by 4-bits
struct HalfByteStreamReader {
    Base128StreamReader& stream;
    size_t read_pos;
    unsigned char tmp;

    HalfByteStreamReader(Base128StreamReader& stream, size_t numblocks=0u)
        : stream(stream)
        , read_pos(0)
        , tmp(0)
    {
    }

    std::tuple<aku_Status, unsigned char> read4bits() {
        if (read_pos % 2 == 0) {
            auto p = stream.read_raw<unsigned char>();
            if (p == nullptr) {
                return std::make_pair(AKU_EOVERFLOW, '\0');
            }
            read_pos++;
            return std::make_tuple(AKU_SUCCESS, tmp & 0xF);
        }
        read_pos++;
        return std::make_tuple(AKU_SUCCESS, tmp >> 4);
    }
};

aku_Status CompressionUtil::compress_doubles(std::vector<ChunkValue> const& input,
                                             Base128StreamWriter&           wstream,
                                             size_t                        *size)
{
    uint64_t prev_in_series = 0ul;
    HalfByteStreamWriter stream(wstream);
    for (size_t ix = 0u; ix != input.size(); ix++) {
        if (input.at(ix).type == ChunkValue::FLOAT) {
            union {
                double real;
                uint64_t bits;
            } curr = {};
            curr.real = input.at(ix).value.floatval;
            uint64_t diff = curr.bits ^ prev_in_series;
            prev_in_series = curr.bits;
            int res = 64;
            if (diff != 0) {
                res = __builtin_clzl(diff);
            }
            int nblocks = 0xF - res / 4;
            if (nblocks < 0) {
                nblocks = 0;
            }
            auto status = stream.add4bits(nblocks);
            if (status == AKU_SUCCESS) {
                for (int i = (nblocks + 1); i --> 0;) {
                    status = stream.add4bits(diff & 0xF);
                    if (status != AKU_SUCCESS) {
                        break;
                    }
                    diff >>= 4;
                }
            }
            if (status != AKU_SUCCESS) {
                return status;
            }
        }
    }
    stream.close();
    *size = stream.write_pos;
    return AKU_SUCCESS;
}

aku_Status CompressionUtil::decompress_doubles(Base128StreamReader& rstream,
                                               size_t               numblocks,
                                               std::vector<double> *output)
{
    aku_Status status = AKU_SUCCESS;
    uint64_t prev_in_series = 0ul;
    HalfByteStreamReader stream(rstream, numblocks);
    size_t ix = 0;
    while(numblocks) {
        uint64_t diff   = 0ul;
        int      nsteps = 0;
        std::tie(status, nsteps) = stream.read4bits();
        if (status != AKU_SUCCESS) {
            return status;
        }
        for (int i = 0; i < (nsteps + 1); i++) {
            uint64_t delta = 0ul;
            std::tie(status, delta) = stream.read4bits();
            if (status != AKU_SUCCESS) {
                return status;
            }
            diff |= delta << (i*4);
        }
        numblocks -= nsteps + 2;  // 1 for (nsteps + 1) and 1 for number of 4bit blocks
        union {
            uint64_t bits;
            double real;
        } curr = {};
        curr.bits = prev_in_series ^ diff;
        output->push_back(curr.real);
        prev_in_series = curr.bits;
        ix++;
    }
    return status;
}

/** NOTE:
  * Data should be ordered by paramid and timestamp.
  * ------------------------------------------------
  * Chunk format:
  * chunk size - uint32 - total number of bytes in the chunk
  * nelements - uint32 - total number of elements in the chunk
  * paramid stream:
  *     stream size - uint32 - number of bytes in a stream
  *     body - array
  * timestamp stream:
  *     stream size - uint32 - number of bytes in a stream
  *     body - array
  * payload stream:
  *     ncolumns - number of columns stored (for future use)
  *     column[0]:
  *         types stream:
  *             stream size - uint32
  *             bytes
  *         double stream:
  *             stream size - uint32
  *             bytes:
  *         lengths stream: (note: blob type)
  *             stream size - uint32
  *             bytes:
  *         offsets stream: (note: blob type)
  *             stream size - uint32
  *             bytes:
  */

aku_Status CompressionUtil::encode_chunk( uint32_t           *n_elements
                                        , aku_Timestamp      *ts_begin
                                        , aku_Timestamp      *ts_end
                                        , ChunkWriter        *writer
                                        , const ChunkHeader&  data)
{
    aku_MemRange available_space = writer->allocate();
    unsigned char* begin = (unsigned char*)available_space.address;
    unsigned char* end = begin + (available_space.length - 2*sizeof(uint32_t));  // 2*sizeof(aku_EntryOffset)
    Base128StreamWriter stream(begin, end);

    // Total size of the chunk
    uint32_t* total_size = stream.allocate<uint32_t>();

    // Number of elements stored
    uint32_t* cardinality = stream.allocate<uint32_t>();
    *cardinality = (uint32_t)data.paramids.size();

    // ParamId stream
    uint32_t* paramid_stream_size = stream.allocate<uint32_t>();
    DeltaRLEWriter paramid_stream(stream);
    for (auto id: data.paramids) {
        auto status = paramid_stream.put(id);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    paramid_stream.commit();
    *paramid_stream_size = (uint32_t)paramid_stream.size();

    // Timestamp stream
    uint32_t* timestamp_stream_size = stream.allocate<uint32_t>();
    DeltaRLEWriter timestamp_stream(stream);
    for (auto ts: data.timestamps) {
        auto status = timestamp_stream.put(ts);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    timestamp_stream.commit();
    *timestamp_stream_size = (uint32_t)timestamp_stream.size();

    // Save number of columns (always 1)
    uint32_t* ncolumns = stream.allocate<uint32_t>();
    if (ncolumns == nullptr) {
        return AKU_EOVERFLOW;
    }
    *ncolumns = 1;
    // Types stream
    uint32_t* types_stream_size = stream.allocate<uint32_t>();
    if (types_stream_size == nullptr) {
        return AKU_EOVERFLOW;
    }
    RLEStreamWriter<int> types_stream(stream);
    for (const auto& item: data.values[col]) {
        int t = item.type;
        auto status = types_stream.put(t);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    types_stream.commit();
    *types_stream_size = (uint32_t)types_stream.size();

    // Doubles stream
    uint32_t* doubles_stream_size = stream.allocate<uint32_t>();
    if (doubles_stream_size == nullptr) {
        return AKU_EOVERFLOW;
    }
    size_t ndobules = 0;
    auto status = CompressionUtil::compress_doubles(data.values[col], stream, &ndoubles);
    if (status != AKU_SUCCESS) {
        return status;
    }
    *doubles_stream_size = (uint32_t)ndoubles;

    // Blob lengths stream
    uint32_t* lengths_stream_size = stream.allocate<uint32_t>();
    if (lengths_stream_size == nullptr) {
        return AKU_EOVERFLOW;
    }
    RLELenWriter len_steram(stream);
    for (const auto& item: data.values[col]) {
        if (item.type == ChunkValue::BLOB) {
            auto status = len_stream.put(item.value.blobval.length);
            if (status != AKU_SUCCESS) {
                return status;
            }
        }
    }
    len_stream.commit();
    *lengths_stream_size = (uint32_t)len_stream.size();

    // Blob offsets stream
    uint32_t* offset_stream_size = stream.allocate<uint32_t>();
    if (offset_stream_size == nullptr) {
        return AKU_EOVERFLOW;
    }
    DeltaRLEWriter offset_stream(stream);
    for (const auto& item: data.values[col]) {
        if (item.type == ChunkValue::BLOB) {
            auto status = offset_stream.put(item.value.blobval.offset);
            if (status != AKU_SUCCESS) {
                return status;
            }
        }
    }
    offset_stream.commit();
    *offset_stream_size = (uint32_t)offset_stream.size();

    // Save metadata
    *total_size = stream.size();
    return writer->commit(stream.size());
}

int CompressionUtil::decode_chunk( ChunkHeader *header
                                 , const unsigned char **pbegin
                                 , const unsigned char *pend
                                 , int stage
                                 , int steps
                                 , uint32_t probe_length)
{
    Base128StreamReader rstream(*pbegin, pend);
    uint32_t bytes_total = rstream.read_raw<uint32_t>();
    uint32_t nelements = rstream.read_raw<uint32_t>();
   /*
    * chunk size - uint32 - total number of bytes in the chunk
    * nelements - uint32 - total number of elements in the chunk
    * paramid stream:
    *     stream size - uint32 - number of bytes in a stream
    *     body - array
    * timestamp stream:
    *     stream size - uint32 - number of bytes in a stream
    *     body - array
    * payload stream:
    *     ncolumns - number of columns stored (for future use)
    *     column[0]:
    *         types stream:
    *             stream size - uint32
    *             bytes
    *         double stream:
    *             stream size - uint32
    *             bytes:
    *         lengths stream: (note: blob type)
    *             stream size - uint32
    *             bytes:
    *         offsets stream: (note: blob type)
    *             stream size - uint32
    *             bytes:
    */
    throw "Not implemented";
}

template<class Fn>
bool reorder_chunk_header(ChunkHeader const& header, ChunkHeader* out, Fn const& f) {
    throw "Not implemented";
}

bool CompressionUtil::convert_from_chunk_order(ChunkHeader const& header, ChunkHeader* out) {
    auto fn = [header](int lhs, int rhs) {
        auto lhstup = std::make_tuple(header.timestamps[lhs], header.paramids[lhs]);
        auto rhstup = std::make_tuple(header.timestamps[rhs], header.paramids[rhs]);
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

bool CompressionUtil::convert_from_time_order(ChunkHeader const& header, ChunkHeader* out) {
    auto fn = [header](int lhs, int rhs) {
        auto lhstup = std::make_tuple(header.paramids[lhs], header.timestamps[lhs]);
        auto rhstup = std::make_tuple(header.paramids[rhs], header.timestamps[rhs]);
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

}
