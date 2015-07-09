#include "compression.h"
#include "util.h"

#include <unordered_map>
#include <algorithm>

namespace Akumuli {

StreamOutOfBounds::StreamOutOfBounds(const char* msg) : std::runtime_error(msg)
{
}

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

    void add4bits(unsigned char value) {
        if (write_pos % 2 == 0) {
            tmp = value & 0xF;
        } else {
            tmp |= (value << 4);
            stream.put(tmp);
            tmp = 0;
        }
        write_pos++;
    }

    void close() {
        if (write_pos % 2 != 0) {
            stream.put(tmp);
        }
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

    unsigned char read4bits() {
        if (read_pos % 2 == 0) {
            tmp = stream.read_raw<unsigned char>();
            read_pos++;
            return tmp & 0xF;
        }
        read_pos++;
        return tmp >> 4;
    }
};

size_t CompressionUtil::compress_doubles(std::vector<ChunkValue> const& input,
                                         Base128StreamWriter&           wstream)
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
            stream.add4bits(nblocks);
            for (int i = (nblocks + 1); i --> 0;) {
                stream.add4bits(diff & 0xF);
                diff >>= 4;
            }
        }
    }
    stream.close();
    return stream.write_pos;
}

void CompressionUtil::decompress_doubles(Base128StreamReader&     rstream,
                                         size_t                   numblocks,
                                         std::vector<ChunkValue> *output)
{
    uint64_t prev_in_series = 0ul;
    HalfByteStreamReader stream(rstream, numblocks);
    auto end = output->end();
    auto it = output->begin();
    while(numblocks) {
        uint64_t diff   = 0ul;
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
        prev_in_series = curr.bits;
        // put
        it = std::find_if(it, end, [](ChunkValue value) { return value.type == ChunkValue::FLOAT; });
        if (it < end) {
            it->value.floatval = curr.real;
            it++;
        } else {
            throw StreamOutOfBounds("can't decode doubles, not enough space inside the chunk");
        }
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

template<class StreamType, class Fn>
aku_Status write_to_stream(Base128StreamWriter& stream, const Fn& writer) {
    uint32_t* length_prefix = stream.allocate<uint32_t>();
    StreamType wstream(stream);
    writer(wstream);
    wstream.commit();
    *length_prefix = (uint32_t)wstream.size();
    return AKU_SUCCESS;
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
    Base128StreamWriter stream(begin, end);

    try {
        // ParamId stream
        write_to_stream<DeltaRLEWriter>(stream, [&](DeltaRLEWriter& paramid_stream) {
            for (auto id: data.paramids) {
                paramid_stream.put(id);
            }
        });

        // Timestamp stream
        write_to_stream<DeltaRLEWriter>(stream, [&](DeltaRLEWriter& timestamp_stream) {
            aku_Timestamp mints = AKU_MAX_TIMESTAMP,
                          maxts = AKU_MIN_TIMESTAMP;
            for (auto ts: data.timestamps) {
                mints = std::min(mints, ts);
                maxts = std::max(maxts, ts);
                timestamp_stream.put(ts);
            }
            *ts_begin = mints;
            *ts_end   = maxts;
        });

        // Save number of columns (always 1)
        uint32_t* ncolumns = stream.allocate<uint32_t>();
        *ncolumns = 1;

        // Types stream
        write_to_stream<RLEStreamWriter<int>>(stream, [&](RLEStreamWriter<int>& types_stream) {
            for (const auto& item: data.values) {
                int t = item.type;
                types_stream.put(t);
            }
        });

        // Doubles stream
        uint32_t* doubles_stream_size = stream.allocate<uint32_t>();
        *doubles_stream_size = (uint32_t)CompressionUtil::compress_doubles(data.values, stream);

        // Blob lengths stream
        write_to_stream<RLELenWriter>(stream, [&](RLELenWriter& len_stream) {
            for (const auto& item: data.values) {
                if (item.type == ChunkValue::BLOB) {
                    len_stream.put(item.value.blobval.length);
                }
            }
        });

        // Blob offsets stream
        write_to_stream<DeltaRLEWriter>(stream, [&](DeltaRLEWriter& offset_stream) {
            for (const auto& item: data.values) {
                if (item.type == ChunkValue::BLOB) {
                    offset_stream.put(item.value.blobval.offset);
                }
            }
        });

        *n_elements = static_cast<uint32_t>(data.paramids.size());
    } catch (StreamOutOfBounds const& e) {
        // TODO: add logging here
        return AKU_EOVERFLOW;
    }

    return writer->commit(stream.size());
}

template<class Stream, class Fn>
void read_from_stream(Base128StreamReader& reader, const Fn& func) {
    uint32_t size_prefix = reader.read_raw<uint32_t>();
    Stream stream(reader);
    func(stream, size_prefix);
}

aku_Status CompressionUtil::decode_chunk( ChunkHeader         *header
                                        , const unsigned char *pbegin
                                        , const unsigned char *pend
                                        , uint32_t             nelements)
{
    try {
        Base128StreamReader rstream(pbegin, pend);
        // Paramids
        read_from_stream<DeltaRLEReader>(rstream, [&](DeltaRLEReader& reader, uint32_t size) {
            for (auto i = nelements; i --> 0;) {
                auto paramid = reader.next();
                header->paramids.push_back(paramid);
            }
        });

        // Timestamps
        read_from_stream<DeltaRLEReader>(rstream, [&](DeltaRLEReader& reader, uint32_t size) {
            for (auto i = nelements; i--> 0;) {
                auto timestamp = reader.next();
                header->timestamps.push_back(timestamp);
            }
        });

        // Payload
        const uint32_t ncolumns = rstream.read_raw<uint32_t>();
        AKU_UNUSED(ncolumns);

        // Types stream
        read_from_stream<RLEStreamReader<int>>(rstream, [&](RLEStreamReader<int>& reader, uint32_t size) {
            for (auto i = nelements; i --> 0;) {
                ChunkValue value = { reader.next() };
                header->values.push_back(value);
            }
        });

        // Doubles stream
        const uint32_t nblocks = rstream.read_raw<uint32_t>();
        CompressionUtil::decompress_doubles(rstream, nblocks, &header->values);

        // Lengths
        read_from_stream<RLELenReader>(rstream, [&](RLELenReader& reader, uint32_t size) {
            for (auto& item: header->values) {
                if (item.type == ChunkValue::BLOB) {
                    auto len = reader.next();
                    item.value.blobval.length = len;
                }
            }
        });

        // Offsets
        read_from_stream<DeltaRLEReader>(rstream, [&](DeltaRLEReader& reader, uint32_t size) {
            for (auto& item: header->values) {
                if (item.type == ChunkValue::BLOB) {
                    auto offset = reader.next();
                    item.value.blobval.offset = offset;
                }
            }
        });
    } catch (StreamOutOfBounds const&) {
        return AKU_EBAD_DATA;
    }
    return AKU_SUCCESS;
}

template<class Fn>
bool reorder_chunk_header(ChunkHeader const& header, ChunkHeader* out, Fn const& f) {
    auto len = header.timestamps.size();
    if (len != header.values.size() || len != header.paramids.size()) {
        return false;
    }
    // prepare indexes
    std::vector<int> index;
    for (auto i = 0u; i < header.timestamps.size(); i++) {
        index.push_back(i);
    }
    std::stable_sort(index.begin(), index.end(), f);
    out->paramids.reserve(index.size());
    out->timestamps.reserve(index.size());
    out->values.reserve(index.size());
    for(auto ix: index) {
        out->paramids.push_back(header.paramids.at(ix));
        out->timestamps.push_back(header.timestamps.at(ix));
        out->values.push_back(header.values.at(ix));
    }
    return true;
}

bool CompressionUtil::convert_from_chunk_order(ChunkHeader const& header, ChunkHeader* out) {
    auto fn = [&header](int lhs, int rhs) {
        auto lhstup = std::make_tuple(header.timestamps[lhs], header.paramids[lhs]);
        auto rhstup = std::make_tuple(header.timestamps[rhs], header.paramids[rhs]);
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

bool CompressionUtil::convert_from_time_order(ChunkHeader const& header, ChunkHeader* out) {
    auto fn = [&header](int lhs, int rhs) {
        auto lhstup = std::make_tuple(header.paramids[lhs], header.timestamps[lhs]);
        auto rhstup = std::make_tuple(header.paramids[rhs], header.timestamps[rhs]);
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

}
