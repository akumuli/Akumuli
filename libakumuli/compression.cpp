#include "compression.h"
#include <unordered_map>

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
                                         std::vector<aku_ParamId> const& params,
                                         ByteVector *buffer)
{
    std::unordered_map<aku_ParamId, uint64_t> prev_in_series;
    for (auto id: params) {
        prev_in_series[id] = 0ul;
    }
    HalfByteStream stream(buffer);
    for (size_t ix = 0u; ix != input.size(); ix++) {
        union {
            double real;
            uint64_t bits;
        } curr = {};
        curr.real = input.at(ix);
        aku_ParamId id = params.at(ix);
        uint64_t prev = prev_in_series.at(id);
        uint64_t diff = curr.bits ^ prev;
        prev_in_series.at(id) = curr.bits;
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
                                         std::vector<aku_ParamId> const& params,
                                         std::vector<double> *output)
{
    std::unordered_map<aku_ParamId, uint64_t> prev_in_series;
    for (auto id: params) {
        prev_in_series[id] = 0ul;
    }
    HalfByteStream stream(&buffer, numblocks);
    size_t ix = 0;
    while(numblocks) {
        aku_ParamId id = params.at(ix);
        uint64_t prev = prev_in_series.at(id);
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
        curr.bits = prev ^ diff;
        output->push_back(curr.real);
        prev_in_series.at(id) = curr.bits;
        ix++;
    }
}

aku_Status CompressionUtil::encode_chunk( uint32_t           *n_elements
                                        , aku_Timestamp      *ts_begin
                                        , aku_Timestamp      *ts_end
                                        , ChunkWriter        *writer
                                        , const ChunkHeader&  data)
{
    // NOTE: it is possible to avoid copying and write directly to page
    // instead of temporary byte vectors
    ByteVector paramids;
    ByteVector timestamps;
    ByteVector offsets;
    ByteVector lengths;

    DeltaRLEWriter paramid_stream(paramids);
    DeltaRLEWriter timestamp_stream(timestamps);
    DeltaRLEWriter offset_stream(offsets);
    RLELenWriter length_stream(lengths);

    std::vector<aku_ParamId> params_with_zlen;

    for (auto i = 0ul; i < data.timestamps.size(); i++) {
        auto pid = data.paramids.at(i);
        auto offset = data.offsets.at(i);
        auto len = data.lengths.at(i);
        auto ts = data.timestamps.at(i);
        paramid_stream.put(pid);
        timestamp_stream.put(ts);
        offset_stream.put(offset);
        length_stream.put(len);
        if (len == 0) {
            params_with_zlen.push_back(pid);
        }
    }

    paramid_stream.close();
    timestamp_stream.close();
    offset_stream.close();
    length_stream.close();

    uint32_t size_estimate =
            static_cast<uint32_t>( paramid_stream.size()
                                 + timestamp_stream.size()
                                 + offset_stream.size()
                                 + length_stream.size()
                                 + sizeof(uint64_t)
                                 );

    aku_Status status = AKU_SUCCESS;

    switch(status) {
    case AKU_SUCCESS:
        // Doubles
        uint64_t nblocks = 0ul;
        if (!data.values.empty()) {
            ByteVector compressed;
            nblocks = CompressionUtil::compress_doubles(data.values, params_with_zlen, &compressed);
            size_estimate += static_cast<uint32_t>(compressed.size());
            const aku_MemRange compressed_mrange = {
                compressed.data(),
                static_cast<uint32_t>(compressed.size())
            };
            status = writer->add_chunk(compressed_mrange, size_estimate);
            if (status != AKU_SUCCESS) {
                break;
            }
            size_estimate -= compressed.size();
        }
        aku_MemRange nblocks_mrange = {
            &nblocks,
            sizeof(nblocks),
        };
        // Doubles size
        status = writer->add_chunk(nblocks_mrange, size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        size_estimate -= sizeof(nblocks);
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
        size_estimate -= length_stream.size();
        // Timestamps
        status = writer->add_chunk(timestamp_stream.get_memrange(), size_estimate);
        if (status != AKU_SUCCESS) {
            break;
        }
        auto ts_stream_size = timestamp_stream.size();
        size_estimate -= ts_stream_size;
        // Param-Ids
        status = writer->add_chunk(paramid_stream.get_memrange(), size_estimate);
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
        // read paramids
        DeltaRLEReader pid_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->paramids.push_back(pid_reader.next());
        }
        *pbegin = pid_reader.pos();
        if (--steps == 0) {
            return 1;
        }
    }
    case 1: {
        // read timestamps
        DeltaRLEReader tst_reader(*pbegin, pend);
        for (auto i = 0u; i < probe_length; i++) {
            header->timestamps.push_back(tst_reader.next());
        }
        *pbegin = tst_reader.pos();
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
        DeltaRLEReader off_reader(*pbegin, pend);
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
        uint64_t nblocks = *reinterpret_cast<const uint64_t*>(*pbegin);
        *pbegin += sizeof(uint64_t);
        if (nblocks) {
            std::vector<aku_ParamId> params;
            for (size_t i = 0; i != header->paramids.size(); i++) {
                if (header->lengths.at(i) == 0) {
                    auto pid = header->paramids.at(i);
                    params.push_back(pid);
                }
            }
            ByteVector buffer(*pbegin, *pbegin + (nblocks/2 + 1));
            CompressionUtil::decompress_doubles(buffer, nblocks, params, &header->values);
            *pbegin += static_cast<uint32_t>(buffer.size());
        }
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
