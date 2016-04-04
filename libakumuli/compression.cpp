#include "compression.h"
#include "util.h"

#include <unordered_map>
#include <algorithm>

namespace Akumuli {

FcmPredictor::FcmPredictor(size_t table_size)
    : last_hash(0ull)
    , MASK_(table_size - 1)
{
    assert((table_size & MASK_) == 0);
    table.resize(table_size);
}

uint64_t FcmPredictor::predict_next() const {
    return table[last_hash];
}

void FcmPredictor::update(uint64_t value) {
    table[last_hash] = value;
    last_hash = ((last_hash << 6) ^ (value >> 48)) & MASK_;
}

//! C-tor. `table_size` should be a power of two.
DfcmPredictor::DfcmPredictor(int table_size)
    : last_hash (0ul)
    , last_value(0ul)
    , MASK_(table_size - 1)
{
   assert((table_size & MASK_) == 0);
   table.resize(table_size);
}

uint64_t DfcmPredictor::predict_next() const {
    return table.at(last_hash) + last_value;
}

void DfcmPredictor::update(uint64_t value) {
    table[last_hash] = value - last_value;
    last_hash = ((last_hash << 2) ^ ((value - last_value) >> 40)) & MASK_;
    last_value = value;
}

static const int PREDICTOR_N = 1 << 10;

static inline bool encode_value(Base128StreamWriter& wstream, uint64_t diff, unsigned char flag) {
    int nbytes = (flag & 7) + 1;
    int nshift = (64 - nbytes*8)*(flag >> 3);
    diff >>= nshift;
    switch(nbytes) {
    case 8:
        if (!wstream.put_raw(diff)) {
            return false;
        }
        break;
    case 7:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 6:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 5:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 4:
        if (!wstream.put_raw(static_cast<uint32_t>(diff & 0xFFFFFFFF))) {
            return false;
        }
        diff >>= 32;
        break;
    case 3:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 2:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
        diff >>= 8;
    case 1:
        if (!wstream.put_raw(static_cast<unsigned char>(diff & 0xFF))) {
            return false;
        }
    }
    return true;
}

static inline uint64_t decode_value(Base128StreamReader& rstream, unsigned char flag) {
    uint64_t diff = 0ul;
    int nbytes = (flag & 7) + 1;
    for (int i = 0; i < nbytes; i++) {
        uint64_t delta = rstream.read_raw<unsigned char>();
        diff |= delta << (i*8);
    }
    int shift_width = (64 - nbytes*8)*(flag >> 3);
    diff <<= shift_width;
    return diff;
}


FcmStreamWriter::FcmStreamWriter(Base128StreamWriter& stream)
    : stream_(stream)
    , predictor_(PREDICTOR_N)
    , prev_diff_(0)
    , prev_flag_(0)
    , nelements_(0)
{
}

bool FcmStreamWriter::tput(double const* values, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (!put(values[i])) {
            return false;
        }
    }
    return commit();
}

bool FcmStreamWriter::put(double value) {
    union {
        double real;
        uint64_t bits;
    } curr = {};
    curr.real = value;
    uint64_t predicted = predictor_.predict_next();
    predictor_.update(curr.bits);
    uint64_t diff = curr.bits ^ predicted;

    int leading_zeros = 64;
    int trailing_zeros = 64;

    if (diff != 0) {
        trailing_zeros = __builtin_ctzl(diff);
    }
    if (diff != 0) {
        leading_zeros = __builtin_clzl(diff);
    }

    int nbytes;
    unsigned char flag;

    if (trailing_zeros > leading_zeros) {
        // this would be the case with low precision values
        nbytes = 8 - trailing_zeros / 8;
        if (nbytes > 0) {
            nbytes--;
        }
        // 4th bit indicates that only leading bytes are stored
        flag = 8 | (nbytes&7);
    } else {
        nbytes = 8 - leading_zeros / 8;
        if (nbytes > 0) {
            nbytes--;
        }
        // zeroed 4th bit indicates that only trailing bytes are stored
        flag = nbytes&7;
    }

    if (nelements_ % 2 == 0) {
        prev_diff_ = diff;
        prev_flag_ = flag;
    } else {
        // we're storing values by pairs to save space
        unsigned char flags = (prev_flag_ << 4) | flag;
        if (!stream_.put_raw(flags)) {
            return false;
        }
        if (!encode_value(stream_, prev_diff_, prev_flag_)) {
            return false;
        }
        if (!encode_value(stream_, diff, flag)) {
            return false;
        }
    }
    nelements_++;
    return true;
}

size_t FcmStreamWriter::size() const { return stream_.size(); }

bool FcmStreamWriter::commit() {
    if (nelements_ % 2 != 0) {
        // `input` contains odd number of values so we should use
        // empty second value that will take one byte in output
        unsigned char flags = prev_flag_ << 4;
        if (!stream_.put_raw(flags)) {
            return false;
        }
        if (!encode_value(stream_, prev_diff_, prev_flag_)) {
            return false;
        }
        if (!encode_value(stream_, 0ull, 0)) {
            return false;
        }
    }
    return stream_.commit();
}

size_t CompressionUtil::compress_doubles(std::vector<double> const& input,
                                         Base128StreamWriter&       wstream)
{
    PredictorT predictor(PREDICTOR_N);
    uint64_t prev_diff = 0;
    unsigned char prev_flag = 0;
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
        if (diff != 0) {
            leading_zeros = __builtin_clzl(diff);
        }

        int nbytes;
        unsigned char flag;

        if (trailing_zeros > leading_zeros) {
            // this would be the case with low precision values
            nbytes = 8 - trailing_zeros / 8;
            if (nbytes > 0) {
                nbytes--;
            }
            // 4th bit indicates that only leading bytes are stored
            flag = 8 | (nbytes&7);
        } else {
            nbytes = 8 - leading_zeros / 8;
            if (nbytes > 0) {
                nbytes--;
            }
            // zeroed 4th bit indicates that only trailing bytes are stored
            flag = nbytes&7;
        }

        if (ix % 2 == 0) {
            prev_diff = diff;
            prev_flag = flag;
        } else {
            // we're storing values by pairs to save space
            unsigned char flags = (prev_flag << 4) | flag;
            wstream.put_raw(flags);
            encode_value(wstream, prev_diff, prev_flag);
            encode_value(wstream, diff, flag);
        }
    }
    if (input.size() % 2 != 0) {
        // `input` contains odd number of values so we should use
        // empty second value that will take one byte in output
        unsigned char flags = prev_flag << 4;
        wstream.put_raw(flags);
        encode_value(wstream, prev_diff, prev_flag);
        encode_value(wstream, 0ull, 0);
    }
    return input.size();
}

FcmStreamReader::FcmStreamReader(Base128StreamReader& stream)
    : stream_(stream)
    , predictor_(PREDICTOR_N)
    , flags_(0)
    , iter_(0)
{
}

double FcmStreamReader::next() {
    unsigned char flag = 0;
    if (iter_++ % 2 == 0) {
        flags_ = (int)stream_.read_raw<unsigned char>();
        flag = static_cast<unsigned char>(flags_ >> 4);
    } else {
        flag = static_cast<unsigned char>(flags_ & 0xF);
    }
    uint64_t diff = decode_value(stream_, flag);
    union {
        uint64_t bits;
        double real;
    } curr = {};
    uint64_t predicted = predictor_.predict_next();
    curr.bits = predicted ^ diff;
    predictor_.update(curr.bits);
    return curr.real;
}

const unsigned char* FcmStreamReader::pos() const { return stream_.pos(); }

void CompressionUtil::decompress_doubles(Base128StreamReader&     rstream,
                                         size_t                   numvalues,
                                         std::vector<double>     *output)
{
    PredictorT predictor(PREDICTOR_N);
    auto end = output->end();
    auto it = output->begin();
    int flags = 0;
    for (auto i = 0u; i < numvalues; i++) {
        unsigned char flag = 0;
        if (i % 2 == 0) {
            flags = (int)rstream.read_raw<unsigned char>();
            flag = static_cast<unsigned char>(flags >> 4);
        } else {
            flag = static_cast<unsigned char>(flags & 0xF);
        }
        uint64_t diff = decode_value(rstream, flag);
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
            // size of the out-buffer should be known beforehand
            AKU_PANIC("can't decode doubles, not enough space inside the out buffer");
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

aku_Status CompressionUtil::encode_block(SeriesSlice* slice, uint8_t* buffer, size_t size) {
    /* Data format:
     *
     * u32   - number of elements
     * u64   - series id
     * vbyte - timestamps (compressed) interleaved with values (compressed)
     *
     */
    static const size_t SPACE_THRESHOLD = 64;  // TODO: get threshold from specific stream
    Base128StreamWriter stream(buffer, buffer + size);
    auto pcount = stream.allocate<uint32_t>();
    auto pseries = stream.allocate<aku_ParamId>();
    *pseries = slice->id;
    size_t slice_size = slice->size;
    DeltaRLEWriter tstream(stream);
    FcmStreamWriter vstream(stream);
    uint32_t count = 0;
    for (size_t ix = slice->offset; ix < slice_size; ix++) {
        aku_Timestamp ts = slice->ts[ix];
        double value = slice->value[ix];
        bool success = false;
        success = tstream.put(ts);
        success = vstream.put(value) & success;
        if (!success) {
            return AKU_EOVERFLOW;
        }
        count++;
        if (stream.space_left() < SPACE_THRESHOLD) {
            success = tstream.commit();
            success = vstream.commit() & success;
            if (!success) {
                // Error during commit phase, buffer is unrecoverable
                return AKU_EOVERFLOW;
            }
            slice->offset = ix + 1;
            break;
        }
    }
    *pcount = count;
    return AKU_SUCCESS;
}

uint32_t CompressionUtil::number_of_elements_in_block(uint8_t const* buffer, size_t buffer_size) {
    Base128StreamReader rdr(buffer, buffer + buffer_size);
    return rdr.read_raw<uint32_t>();
}

aku_Status CompressionUtil::decode_block(uint8_t const* buffer, size_t buffer_size,
                                         SeriesSlice* dest) {
    // `dest` should have enough space to store the data
    Base128StreamReader stream(buffer, buffer + buffer_size);
    uint32_t nitems = stream.read_raw<uint32_t>();
    aku_ParamId id = stream.read_raw<aku_ParamId>();
    dest->id = id;
    size_t offset = dest->offset;
    if (dest->size < dest->offset || (dest->size - offset) < nitems) {
        return AKU_EBAD_ARG;
    }

    DeltaRLEReader tstream(stream);   // timestamps stream
    FcmStreamReader vstream(stream);  // values stream

    for (size_t i = 0; i < nitems; i++) {
        aku_Timestamp ts = tstream.next();
        double value = vstream.next();
        dest->ts[offset + i] = ts;
        dest->value[offset + i] = value;
    }
    dest->offset = offset + nitems;

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
    } catch (...) {
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
    } catch (...) {
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
