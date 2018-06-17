#include "compression.h"
#include "util.h"

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
