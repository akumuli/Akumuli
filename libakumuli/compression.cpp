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

aku_Status CompressionUtil::encode_chunk( uint32_t           *n_elements
                                        , aku_Timestamp      *ts_begin
                                        , aku_Timestamp      *ts_end
                                        , ChunkWriter        *writer
                                        , const ChunkHeader&  data)
{
    aku_MemRange available_space = writer->allocate();
    unsigned char* begin = (unsigned char*)available_space.address;
    unsigned char* end = begin + (available_space.length - 2*sizeof(uint32_t));  // 2*sizeof(aku_EntryOffset)
    DeltaRLEWriter paramid_stream(begin, end);
    for (auto id: data.paramids) {
        auto status = paramid_stream.put(id);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    paramid_stream.close();

    // new stream writer should continue to use memory
    // used by previous stream writer
    DeltaRLEWriter timestamp_stream(paramid_stream);
    for (auto ts: data.timestamps) {
        auto status = timestamp_stream.put(ts);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    timestamp_stream.close();
    // TODO: implement compression
    return AKU_ENOT_IMPLEMENTED;
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
