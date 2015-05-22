#include "compression.h"
#include <unordered_map>
#include <algorithm>

namespace Akumuli {

//! Stream that can be used to read/write data by 4-bits
struct HalfByteStream {
    ByteVector *data;
    size_t write_pos;
    size_t read_pos;
    unsigned char tmp;

    HalfByteStream(ByteVector *d, size_t numblocks=0u) :
        data(d),
        write_pos(numblocks),
        read_pos(0),
        tmp(0) {
    }

    void add4bits(unsigned char value) {
        if (write_pos % 2 == 0) {
            tmp = value & 0xF;
        } else {
            tmp |= (value << 4);
            data->push_back(tmp);
            tmp = 0;
        }
        write_pos++;
    }

    unsigned char read4bits() {
        if (read_pos % 2 == 0) {
            return data->at(read_pos++ / 2) & 0xF;
        }
        return data->at(read_pos++ / 2) >> 4;
    }

    void close() {
        if (write_pos % 2 != 0) {
            data->push_back(tmp);
        }
    }
};

size_t CompressionUtil::compress_doubles(std::vector<double> const& input,
                                         ByteVector *buffer)
{
    uint64_t prev_in_series = 0ul;
    HalfByteStream stream(buffer);
    for (size_t ix = 0u; ix != input.size(); ix++) {
        union {
            double real;
            uint64_t bits;
        } curr = {};
        curr.real = input.at(ix);
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
        stream.add4bits(nblocks);
        for (int i = (nblocks + 1); i --> 0;) {
            stream.add4bits(diff & 0xF);
            diff >>= 4;
        }
    }
    stream.close();
    return stream.write_pos;
}

void CompressionUtil::decompress_doubles(ByteVector& buffer,
                                         size_t numblocks,
                                         std::vector<double> *output)
{
    uint64_t prev_in_series = 0ul;
    HalfByteStream stream(&buffer, numblocks);
    size_t ix = 0;
    while(numblocks) {
        uint64_t diff = 0ul;
        int nsteps = stream.read4bits();
        for (int i = 0; i < (nsteps + 1); i++) {
            uint64_t delta = stream.read4bits();
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
  *     ncolumns - number of columns stored
  *     column[0]:
  *         types stream:
  *             stream size - uint32
  *             bytes
  *         int stream:  (note: gaps not stored)
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
  *     optional columns 1-n with the same layout
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

    // Columns
    const uint32_t NCOLUMNS = data.longest_row;
    // Save number of columns
    uint32_t* ncolumns = stream.allocate<uint32_t>();
    if (ncolumns == nullptr) {
        return AKU_EOVERFLOW;
    }
    *ncolumns = NCOLUMNS;
    for (int col = 0; col < NCOLUMNS; col++) {
        // Save types stream
        uint32_t* types_stream_size = stream.allocate<uint32_t>();
        if (types_stream_size == nullptr) {
            return AKU_EOVERFLOW;
        }
        RLEStreamWriter<int> types_stream(stream);
        for (const auto& item: data.table[col]) {
            int t = item.type;
            auto status = types_stream.put(t);
            if (status != AKU_SUCCESS) {
                return status;
            }
        }
        types_stream.commit();
        *types_stream_size = (uint32_t)itypes_stream.size();

        // Save int stream
        uint32_t* ints_stream_size = stream.allocate<uint32_t>();
        DeltaRLEWriter<int64_t> ints_stream(stream);
        for (const auto& item: data.table[col]) {
            if (item.type == HeaderCell::INT) {
                auto status = ints_stream.put(item.value.intval);
                if (status != AKU_SUCCESS) {
                    return status;
                }
            }
        }
        ints_stream.commit();
        *ints_stream_size = (uint32_t)ints_stream.size();

        /*         double stream:
         *             stream size - uint32
         *             bytes:
         *         lengths stream: (note: blob type)
         *             stream size - uint32
         *             bytes:
         *         offsets stream: (note: blob type)
         *             stream size - uint32
         *             bytes:
         */
        ...
    }

    // Save metadata
    *total_size = stream.size();
    return AKU_SUCCESS;
}

int CompressionUtil::decode_chunk( ChunkHeader *header
                                 , const unsigned char **pbegin
                                 , const unsigned char *pend
                                 , int stage
                                 , int steps
                                 , uint32_t probe_length)
{
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
