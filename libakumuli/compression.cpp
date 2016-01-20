#include "compression.h"
#include "util.h"

#include <unordered_map>
#include <algorithm>
#include <iostream> // TODO: remove me

namespace Akumuli {

StreamOutOfBounds::StreamOutOfBounds(const char* msg) : std::runtime_error(msg)
{
}

struct PrevValPredictor {
    uint64_t last_value;
    PrevValPredictor(int) : last_value(0)
    {
    }
    uint64_t predict_next() const {
        return last_value;
    }
    void update(uint64_t value) {
        last_value = value;
    }
};

struct FcmPredictor {
    std::vector<uint64_t> table;
    uint64_t last_hash;
    const uint64_t MASK_;

    FcmPredictor(size_t table_size)
        : last_hash(0ull)
        , MASK_(table_size - 1)
    {
        assert((table_size & MASK_) == 0);
        table.resize(table_size);
    }

    uint64_t predict_next() const {
        return table[last_hash];
    }

    void update(uint64_t value) {
        table[last_hash] = value;
        last_hash = ((last_hash << 6) ^ (value >> 48)) & MASK_;
    }
};

struct DfcmPredictor {
    std::vector<uint64_t> table;
    uint64_t last_hash;
    uint64_t last_value;
    const uint64_t MASK_;

    //! C-tor. `table_size` should be a power of two.
    DfcmPredictor(int table_size)
        : last_hash (0ul)
        , last_value(0ul)
        , MASK_(table_size - 1)
    {
       assert((table_size & MASK_) == 0);
       table.resize(table_size);
    }

    uint64_t predict_next() const {
        return table.at(last_hash) + last_value;
    }

    void update(uint64_t value) {
        table[last_hash] = value - last_value;
        last_hash = ((last_hash << 2) ^ ((value - last_value) >> 40)) & MASK_;
        last_value = value;
    }
};

typedef DfcmPredictor PredictorT;

static const int PREDICTOR_N = 1 << 10;

size_t CompressionUtil::compress_doubles(std::vector<double> const& input,
                                         Base128StreamWriter&       wstream)
{
    size_t total_blocks = 0u;
    PredictorT predictor(PREDICTOR_N);
    for (size_t ix = 0u; ix != input.size(); ix++) {
        union {
            double real;
            uint64_t bits;
        } curr = {};
        curr.real = input.at(ix);
        uint64_t predicted = predictor.predict_next();
        predictor.update(curr.bits);
        uint64_t diff = curr.bits ^ predicted;

        int leading_zeros = 64;
        int trailing_zeros = 64;

        if (diff != 0) {
            trailing_zeros = __builtin_ctzl(diff);
        }

        trailing_zeros /= 4;
        diff >>= trailing_zeros*4;
        // trailing_zeros is in [0, 16] range

        if (diff != 0) {
            leading_zeros = __builtin_clzl(diff);
        }

        int nblocks = 8 - leading_zeros / 8;
        if (nblocks > 0) {
            nblocks--;
        }

        unsigned char flags = nblocks&7;
        flags |= trailing_zeros << 3;
        wstream.put_raw(flags);

        // Unrolled loop:
        switch(nblocks) {
        case 7:
            wstream.put_raw(diff);
            total_blocks += (size_t)nblocks + 2;
            break;
        case 6:
            wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
            diff >>= 8;
        case 5:
            wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
            diff >>= 8;
        case 4:
            wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
            diff >>= 8;
        case 3:
            wstream.put_raw(static_cast<uint32_t>(diff & 0xFFFFFFFF));
            total_blocks += (size_t)nblocks + 2;
            break;
        case 2:
            wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
            diff >>= 8;
        case 1:
            wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
            diff >>= 8;
        case 0:
            if (trailing_zeros != 0x10) {
                wstream.put_raw(static_cast<unsigned char>(diff & 0xFF));
                diff >>= 8;
                total_blocks += (size_t)nblocks + 2;
            } else {
                // In this case no payload data is stored
                total_blocks += 1;
            }
        }
    }
    return total_blocks;
}

void CompressionUtil::decompress_doubles(Base128StreamReader&     rstream,
                                         size_t                   numblocks,
                                         std::vector<double>     *output)
{
    PredictorT predictor(PREDICTOR_N);
    auto end = output->end();
    auto it = output->begin();
    while(numblocks) {
        uint64_t diff = 0ul;
        int flags = (int)rstream.read_raw<unsigned char>();
        int nsteps = (flags & 7) + 1;
        int trailing_zeros = flags >> 3;
        if (trailing_zeros != 0x10) {
            for (int i = 0; i < nsteps; i++) {
                uint64_t delta = rstream.read_raw<unsigned char>();
                diff |= delta << (i*8);
            }
        }
        diff <<= trailing_zeros*4;
        numblocks -= nsteps + 1;
        union {
            uint64_t bits;
            double real;
        } curr = {};
        uint64_t predicted = predictor.predict_next();
        curr.bits = predicted ^ diff;
        predictor.update(curr.bits);
        // put
        if (it < end) {
            *it++ = curr.real;
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
  *         double stream:
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
                                        , const UncompressedChunk&  data)
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

        // Doubles stream
        uint32_t* doubles_stream_size = stream.allocate<uint32_t>();
        *doubles_stream_size = (uint32_t)CompressionUtil::compress_doubles(data.values, stream);

        *n_elements = static_cast<uint32_t>(data.paramids.size());
    } catch (StreamOutOfBounds const& e) {
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

aku_Status CompressionUtil::decode_chunk( UncompressedChunk   *header
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

        // Doubles stream
        header->values.resize(nelements);
        const uint32_t nblocks = rstream.read_raw<uint32_t>();
        CompressionUtil::decompress_doubles(rstream, nblocks, &header->values);
    } catch (StreamOutOfBounds const&) {
        return AKU_EBAD_DATA;
    }
    return AKU_SUCCESS;
}

template<class Fn>
bool reorder_chunk_header(UncompressedChunk const& header, UncompressedChunk* out, Fn const& f) {
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

bool CompressionUtil::convert_from_chunk_order(UncompressedChunk const& header, UncompressedChunk* out) {
    auto fn = [&header](int lhs, int rhs) {
        auto lhstup = header.timestamps[lhs];
        auto rhstup = header.timestamps[rhs];
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

bool CompressionUtil::convert_from_time_order(UncompressedChunk const& header, UncompressedChunk* out) {
    auto fn = [&header](int lhs, int rhs) {
        auto lhstup = header.paramids[lhs];
        auto rhstup = header.paramids[rhs];
        return lhstup < rhstup;
    };
    return reorder_chunk_header(header, out, fn);
}

}
