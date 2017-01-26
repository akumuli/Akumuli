#include "compression.h"
#include "util.h"
#include "akumuli_version.h"

#include <unordered_map>
#include <algorithm>
#include <iostream>

namespace Akumuli {

SimplePredictor::SimplePredictor(size_t) : last_value(0) {}

u64 SimplePredictor::predict_next() const {
    return last_value;
}

void SimplePredictor::update(u64 value) {
    last_value = value;
}

FcmPredictor::FcmPredictor(size_t table_size)
    : last_hash(0ull)
    , MASK_(table_size - 1)
{
    assert((table_size & MASK_) == 0);
    table.resize(table_size);
}

u64 FcmPredictor::predict_next() const {
    return table[last_hash];
}

void FcmPredictor::update(u64 value) {
    table[last_hash] = value;
    last_hash = ((last_hash << 5) ^ (value >> 50)) & MASK_;
}

//! C-tor. `table_size` should be a power of two.
DfcmPredictor::DfcmPredictor(int table_size)
    : last_hash (0ul)
    , last_value(0ul)
    , MASK_(static_cast<u64>(table_size - 1))
{
   assert((table_size & MASK_) == 0);
   table.resize(static_cast<u64>(table_size));
}

u64 DfcmPredictor::predict_next() const {
    return table.at(last_hash) + last_value;
}

void DfcmPredictor::update(u64 value) {
    table[last_hash] = value - last_value;
    last_hash = ((last_hash << 5) ^ ((value - last_value) >> 50)) & MASK_;
    last_value = value;
}

static const int PREDICTOR_N = 1 << 7;

template<class StreamT>
static inline bool encode_value(StreamT& wstream, u64 diff, unsigned char flag) {
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
        if (!wstream.put_raw(static_cast<u32>(diff & 0xFFFFFFFF))) {
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

template<class StreamT>
static inline u64 decode_value(StreamT& rstream, unsigned char flag) {
    u64 diff = 0ul;
    int nbytes = (flag & 7) + 1;
    for (int i = 0; i < nbytes; i++) {
        u64 delta = rstream.template read_raw<unsigned char>();
        diff |= delta << (i*8);
    }
    int shift_width = (64 - nbytes*8)*(flag >> 3);
    diff <<= shift_width;
    return diff;
}


        // ///////////////////////////////// //
        // FcmStreamWriter & FcmStreamReader //
        // ///////////////////////////////// //

FcmStreamWriter::FcmStreamWriter(VByteStreamWriter& stream)
    : stream_(stream)
    , predictor_(PREDICTOR_N)
    , prev_diff_(0)
    , prev_flag_(0)
    , nelements_(0)
{
}


bool FcmStreamWriter::tput(double const* values, size_t n) {
    assert(n == 16);
    u8  flags[16];
    u64 diffs[16];
    for (u32 i = 0; i < n; i++) {
        std::tie(diffs[i], flags[i]) = encode(values[i]);
    }
    u64 sum_diff = 0;
    for (u32 i = 0; i < n; i++) {
        sum_diff |= diffs[i];
    }
    if (sum_diff == 0) {
        // Shortcut
        if (!stream_.put_raw((u8)0xFF)) {
            return false;
        }
    } else {
        for (size_t i = 0; i < n; i+=2) {
            u64 prev_diff, curr_diff;
            unsigned char prev_flag, curr_flag;
            prev_diff = diffs[i];
            curr_diff = diffs[i+1];
            prev_flag = flags[i];
            curr_flag = flags[i+1];
            if (curr_flag == 0xF) {
                curr_flag = 0;
            }
            if (prev_flag == 0xF) {
                prev_flag = 0;
            }
            unsigned char flags = static_cast<unsigned char>((prev_flag << 4) | curr_flag);
            if (!stream_.put_raw(flags)) {
                return false;
            }
            if (!encode_value(stream_, prev_diff, prev_flag)) {
                return false;
            }
            if (!encode_value(stream_, curr_diff, curr_flag)) {
                return false;
            }
        }
    }
    return commit();
}

std::tuple<u64, unsigned char> FcmStreamWriter::encode(double value) {
    union {
        double real;
        u64 bits;
    } curr = {};
    curr.real = value;
    u64 predicted = predictor_.predict_next();
    predictor_.update(curr.bits);
    u64 diff = curr.bits ^ predicted;

    // Number of trailing and leading zero-bytes
    int leading_bytes = 8;
    int trailing_bytes = 8;

    if (diff != 0) {
        trailing_bytes = __builtin_ctzl(diff) / 8;
        leading_bytes = __builtin_clzl(diff) / 8;
    } else {
        // Fast path for 0-diff values.
        // Flags 7 and 15 are interchangeable.
        // If there is 0 trailing zero bytes and 0 leading bytes
        // code will always generate flag 7 so we can use flag 17
        // for something different (like 0 indication)
        return std::make_tuple(0, 0xF);
    }

    int nbytes;
    unsigned char flag;

    if (trailing_bytes > leading_bytes) {
        // this would be the case with low precision values
        nbytes = 8 - trailing_bytes;
        if (nbytes > 0) {
            nbytes--;
        }
        // 4th bit indicates that only leading bytes are stored
        flag = 8 | (nbytes&7);
    } else {
        nbytes = 8 - leading_bytes;
        if (nbytes > 0) {
            nbytes--;
        }
        // zeroed 4th bit indicates that only trailing bytes are stored
        flag = nbytes&7;
    }
    return std::make_tuple(diff, flag);
}

bool FcmStreamWriter::put(double value) {
    u64 diff;
    unsigned char flag;
    std::tie(diff, flag) = encode(value);
    if (flag == 0xF) {
        flag = 0;  // Just store one byte, space opt. is disabled
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
                                         Base128StreamWriter &wstream)
{
    PredictorT predictor(PREDICTOR_N);
    u64 prev_diff = 0;
    u8 prev_flag = 0;
    for (size_t ix = 0u; ix != input.size(); ix++) {
        union {
            double real;
            u64 bits;
        } curr = {};
        curr.real = input.at(ix);
        u64 predicted = predictor.predict_next();
        predictor.update(curr.bits);
        u64 diff = curr.bits ^ predicted;

        int leading_zeros = 64;
        int trailing_zeros = 64;

        if (diff != 0) {
            trailing_zeros = __builtin_ctzl(diff);
        }
        if (diff != 0) {
            leading_zeros = __builtin_clzl(diff);
        }

        int nbytes;
        u8 flag;

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
            u8 flags = static_cast<u8>((prev_flag << 4) | flag);
            bool success = wstream.put_raw(flags);
            success &= encode_value(wstream, prev_diff, prev_flag);
            success &= encode_value(wstream, diff, flag);
            if (!success) {
                std::runtime_error error("Buffer is too small");
                BOOST_THROW_EXCEPTION(error);
            }
        }
    }
    if (input.size() % 2 != 0) {
        // `input` contains odd number of values so we should use
        // empty second value that will take one byte in output
        u8 flags = static_cast<u8>(prev_flag << 4);
        bool success = wstream.put_raw(flags);
        success &= encode_value(wstream, prev_diff, prev_flag);
        success &= encode_value(wstream, 0ull, 0);
        if (!success) {
            std::runtime_error error("Buffer is too small");
            BOOST_THROW_EXCEPTION(error);
        }
    }
    return input.size();
}

FcmStreamReader::FcmStreamReader(VByteStreamReader& stream)
    : stream_(stream)
    , predictor_(PREDICTOR_N)
    , flags_(0)
    , iter_(0)
    , nzeroes_(0)
{
}

double FcmStreamReader::next() {
    unsigned char flag = 0;
    if (iter_++ % 2 == 0 && nzeroes_ == 0) {
        flags_ = static_cast<u32>(stream_.read_raw<u8>());
        if (flags_ == 0xFF) {
            // Shortcut
            nzeroes_ = 16;
        }
        flag = static_cast<unsigned char>(flags_ >> 4);
    } else {
        flag = static_cast<unsigned char>(flags_ & 0xF);
    }
    u64 diff;
    if (nzeroes_ == 0) {
        diff = decode_value(stream_, flag);
    } else {
        diff = 0ull;
        nzeroes_--;
    }
    union {
        u64 bits;
        double real;
    } curr = {};
    u64 predicted = predictor_.predict_next();
    curr.bits = predicted ^ diff;
    predictor_.update(curr.bits);
    return curr.real;
}

const u8 *FcmStreamReader::pos() const { return stream_.pos(); }

void CompressionUtil::decompress_doubles(Base128StreamReader &rstream,
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
        u64 diff = decode_value(rstream, flag);
        union {
            u64 bits;
            double real;
        } curr = {};
        u64 predicted = predictor.predict_next();
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
    u32* length_prefix = stream.allocate<u32>();
    StreamType wstream(stream);
    writer(wstream);
    wstream.commit();
    *length_prefix = (u32)wstream.size();
    return AKU_SUCCESS;
}


aku_Status CompressionUtil::encode_chunk( u32           *n_elements
                                        , aku_Timestamp      *ts_begin
                                        , aku_Timestamp      *ts_end
                                        , ChunkWriter        *writer
                                        , const UncompressedChunk&  data)
{
    aku_MemRange available_space = writer->allocate();
    unsigned char* begin = (unsigned char*)available_space.address;
    unsigned char* end = begin + (available_space.length - 2*sizeof(u32));  // 2*sizeof(aku_EntryOffset)
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
        u32* ncolumns = stream.allocate<u32>();
        *ncolumns = 1;

        // Doubles stream
        u32* doubles_stream_size = stream.allocate<u32>();
        *doubles_stream_size = (u32)CompressionUtil::compress_doubles(data.values, stream);

        *n_elements = static_cast<u32>(data.paramids.size());
    } catch (...) {
        return AKU_EOVERFLOW;
    }

    return writer->commit(stream.size());
}

template<class Stream, class Fn>
void read_from_stream(Base128StreamReader& reader, const Fn& func) {
    u32 size_prefix = reader.read_raw<u32>();
    Stream stream(reader);
    func(stream, size_prefix);
}

aku_Status CompressionUtil::decode_chunk( UncompressedChunk   *header
                                        , const unsigned char *pbegin
                                        , const unsigned char *pend
                                        , u32             nelements)
{
    try {
        Base128StreamReader rstream(pbegin, pend);
        // Paramids
        read_from_stream<DeltaRLEReader>(rstream, [&](DeltaRLEReader& reader, u32 size) {
            for (auto i = nelements; i --> 0;) {
                auto paramid = reader.next();
                header->paramids.push_back(paramid);
            }
        });

        // Timestamps
        read_from_stream<DeltaRLEReader>(rstream, [&](DeltaRLEReader& reader, u32 size) {
            for (auto i = nelements; i--> 0;) {
                auto timestamp = reader.next();
                header->timestamps.push_back(timestamp);
            }
        });

        // Payload
        const u32 ncolumns = rstream.read_raw<u32>();
        AKU_UNUSED(ncolumns);

        // Doubles stream
        header->values.resize(nelements);
        const u32 nblocks = rstream.read_raw<u32>();
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

namespace StorageEngine {

DataBlockWriter::DataBlockWriter()
    : stream_(nullptr, nullptr)
    , ts_stream_(stream_)
    , val_stream_(stream_)
    , write_index_(0)
    , nchunks_(nullptr)
    , ntail_(nullptr)
{
}

DataBlockWriter::DataBlockWriter(aku_ParamId id, u8 *buf, int size)
    : stream_(buf, buf + size)
    , ts_stream_(stream_)
    , val_stream_(stream_)
    , write_index_(0)
{
    // offset 0
    auto success = stream_.put_raw<u16>(AKUMULI_VERSION);
    // offset 2
    nchunks_ = stream_.allocate<u16>();
    // offset 4
    ntail_ = stream_.allocate<u16>();
    // offset 6
    success = stream_.put_raw(id) && success;
    if (!success || nchunks_ == nullptr || ntail_ == nullptr) {
        AKU_PANIC("Buffer is too small (3)");
    }
    *ntail_ = 0;
    *nchunks_ = 0;
}

aku_Status DataBlockWriter::put(aku_Timestamp ts, double value) {
    if (room_for_chunk()) {
        // Invariant 1: number of elements stored in write buffer (ts_writebuf_ val_writebuf_)
        // equals `write_index_ % CHUNK_SIZE`.
        ts_writebuf_[write_index_ & CHUNK_MASK] = ts;
        val_writebuf_[write_index_ & CHUNK_MASK] = value;
        write_index_++;
        if ((write_index_ & CHUNK_MASK) == 0) {
            // put timestamps
            if (ts_stream_.tput(ts_writebuf_, CHUNK_SIZE)) {
                if (val_stream_.tput(val_writebuf_, CHUNK_SIZE)) {
                    *nchunks_ += 1;
                    return AKU_SUCCESS;
                }
            }
            // Content of the write buffer was lost, this can happen only if `room_for_chunk`
            // function estimates required space incorrectly.
            assert(false);
            return AKU_EOVERFLOW;
        }
    } else {
        // Put values to the end of the stream without compression.
        // This can happen first only when write buffer is empty.
        assert((write_index_ & CHUNK_MASK) == 0);
        if (stream_.put_raw(ts)) {
            if (stream_.put_raw(value)) {
                *ntail_ += 1;
                return AKU_SUCCESS;
            }
        }
        return AKU_EOVERFLOW;
    }
    return AKU_SUCCESS;
}

size_t DataBlockWriter::commit() {
    // It should be possible to store up to one million chunks in one block,
    // for 4K block size this is more then enough.
    auto nchunks = write_index_ / CHUNK_SIZE;
    auto buftail = write_index_ % CHUNK_SIZE;
    // Invariant 2: if DataBlockWriter was closed after `put` method overflowed (return AKU_EOVERFLOW),
    // then `ntail_` should be GE then zero and write buffer should be empty (write_index_ = multiple of CHUNK_SIZE).
    // Otherwise, `ntail_` should be zero.
    if (buftail) {
        // Write buffer is not empty
        if (*ntail_ != 0) {
            // invariant is broken
            AKU_PANIC("Write buffer is not empty but can't be flushed");
        }
        for (int ix = 0; ix < buftail; ix++) {
            auto success = stream_.put_raw(ts_writebuf_[ix]);
            success = stream_.put_raw(val_writebuf_[ix]) && success;
            if (!success) {
                // Data loss. This should never happen at this point. If this error
                // occures then `room_for_chunk` estimates space requirements incorrectly.
                assert(false);
                break;
            }
            *ntail_ += 1;
            write_index_--;
        }
    }
    assert(nchunks <= 0xFFFF);
    *nchunks_ = static_cast<u16>(nchunks);
    return stream_.size();
}

bool DataBlockWriter::room_for_chunk() const {
    static const size_t MARGIN = 10*16 + 9*16;  // worst case
    auto free_space = stream_.space_left();
    if (free_space < MARGIN) {
        return false;
    }
    return true;
}

void DataBlockWriter::read_tail_elements(std::vector<aku_Timestamp>* timestamps,
                                         std::vector<double>* values) const {
    // Note: this method can be used to read values from
    // write buffer. It sort of breaks incapsulation but
    // we don't need  to maintain  another  write buffer
    // anywhere else.
    auto tailsize = write_index_ & CHUNK_MASK;
    for (int i = 0; i < tailsize; i++) {
        timestamps->push_back(ts_writebuf_[i]);
        values->push_back(val_writebuf_[i]);
    }
}

int DataBlockWriter::get_write_index() const {
    // Note: we need to be able to read this index to
    // get rid of write index inside NBTreeLeaf.
    if (!stream_.empty()) {
        return *ntail_ + write_index_;
    }
    return 0;
}

// ////////////////////////////// //
// DataBlockReader implementation //
// ////////////////////////////// //

DataBlockReader::DataBlockReader(u8 const* buf, size_t bufsize)
    : begin_(buf)
    , stream_(buf + DataBlockWriter::HEADER_SIZE, buf + bufsize)
    , ts_stream_(stream_)
    , val_stream_(stream_)
    , read_buffer_{}
    , read_index_(0)
{
    assert(bufsize > 13);
}

static u16 get_block_version(const u8* pdata) {
    u16 version = *reinterpret_cast<const u16*>(pdata);
    return version;
}

static u32 get_main_size(const u8* pdata) {
    u16 main = *reinterpret_cast<const u16*>(pdata + 2);
    return static_cast<u32>(main) * DataBlockReader::CHUNK_SIZE;
}

static u32 get_total_size(const u8* pdata) {
    u16 main = *reinterpret_cast<const u16*>(pdata + 2);
    u16 tail = *reinterpret_cast<const u16*>(pdata + 4);
    return tail + static_cast<u32>(main) * DataBlockReader::CHUNK_SIZE;
}

static aku_ParamId get_block_id(const u8* pdata) {
    aku_ParamId id = *reinterpret_cast<const aku_ParamId*>(pdata + 6);
    return id;
}

std::tuple<aku_Status, aku_Timestamp, double> DataBlockReader::next() {
    if (read_index_ < get_main_size(begin_)) {
        auto chunk_index = read_index_++ & CHUNK_MASK;
        if (chunk_index == 0) {
            // read all timestamps
            for (int i = 0; i < CHUNK_SIZE; i++) {
                read_buffer_[i] = ts_stream_.next();
            }
        }
        double value = val_stream_.next();
        return std::make_tuple(AKU_SUCCESS, read_buffer_[chunk_index], value);
    } else {
        // handle tail values
        if (read_index_ < get_total_size(begin_)) {
            read_index_++;
            auto ts = stream_.read_raw<aku_Timestamp>();
            auto value = stream_.read_raw<double>();
            return std::make_tuple(AKU_SUCCESS, ts, value);
        }
    }
    return std::make_tuple(AKU_ENO_DATA, 0ull, 0.0);
}

size_t DataBlockReader::nelements() const {
    return get_total_size(begin_);
}

aku_ParamId DataBlockReader::get_id() const {
    return get_block_id(begin_);
}

u16 DataBlockReader::version() const {
    return get_block_version(begin_);
}

}

}
