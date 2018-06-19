/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#pragma once
// stdlib
#include <cstdint>
#include <future>
#include <memory>

// libraries
#include <apr.h>
#include <apr_file_io.h>
#include <apr_general.h>

// project
#include "akumuli.h"
#include "util.h"
#include "volumeregistry.h"

namespace Akumuli {
namespace StorageEngine {

//! Address of the block inside storage
typedef u64 LogicAddr;

//! This value represents empty addr. It's too large to be used as a real block addr.
static const LogicAddr EMPTY_ADDR = std::numeric_limits<LogicAddr>::max();

//! Address of the block inside volume (index of the block)
typedef u32 BlockAddr;
enum { AKU_BLOCK_SIZE = 4096 };

struct IOVecBlock {
    enum {
        NCOMPONENTS = 4,
        COMPONENT_SIZE = AKU_BLOCK_SIZE / NCOMPONENTS,
    };

    std::vector<u8>  data_[NCOMPONENTS];
    int pos_;  //! write pos
    LogicAddr addr_;

    /**
     * @brief Create empty IOVecBlock
     * All storage components wouldn't be allocated, pos_
     * will be set to 0.
     */
    IOVecBlock();

    /**
     * @brief Create allocated IOVecBlock
     * AKU_BLOCK_SIZE bytes will be allocated for the first storage
     * component, pos_ will be set to AKU_BLOCK_SIZE.
     * The parameter value doesn't actually matter (used to distingwish between the c-tor's).
     * The block is not writable. Methods `get` and `get_raw` will work.
     */
    IOVecBlock(bool);

    /** Add component if block is less than NCOMPONENTS in size.
     *  Return index of the component or -1 if block is full.
     */
    int add();

    void set_addr(LogicAddr addr);

    LogicAddr get_addr() const;

    int space_left() const;

    //! Remaining bytes to read from offset to current write pos_
    int bytes_to_read(u32 offset) const;

    int size() const;

    void put(u8 val);

    u8 get(u32 offset) const;

    bool safe_put(u8 val);

    int get_write_pos() const;

    void set_write_pos(int pos);

    template<class POD>
    void put(const POD& data) {
        const u8* it = reinterpret_cast<const u8*>(&data);
        for (u32 i = 0; i < sizeof(POD); i++) {
            put(it[i]);
        }
    }

    template<class POD>
    POD get_raw(u32 offset) const {
        const u32 sz = sizeof(POD);
        union {
            POD retval;
            u8 bits[sz];
        } raw;
        for (u32 i = 0; i < sz; i++) {
            raw.bits[i] = get(offset + i);
        }
        return raw.retval;
    }

    //! Allocate memory inside the stream (at the current write position)
    template<class POD>
    POD* allocate() {
        int c = pos_ / COMPONENT_SIZE;
        int i = pos_ % COMPONENT_SIZE;
        if (c >= NCOMPONENTS) {
            return nullptr;
        }
        if (data_[c].empty()) {
            data_[c].resize(COMPONENT_SIZE);
        }
        if ((data_[c].size() - static_cast<u32>(i)) < sizeof(POD)) {
            return nullptr;
        }
        POD* result = reinterpret_cast<POD*>(data_[c].data() + i);
        pos_ += sizeof(POD);
        return result;
    }

    //! Allocate memory inside the stream (at the current write position)
    u8* allocate(u32 size);

    //-----

    bool is_readonly() const;

    const u8* get_data(int component) const;

    const u8* get_cdata(int component) const;

    u8* get_data(int component);

    size_t get_size(int component) const;
};

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;


/** Class that represents metadata volume.
  * MetaVolume is a file that contains some information
  * about each regullar volume - write position, generation, etc.
  *
  * Hardware asumptions. At this point we assume that disck sector size is
  * 4KB and sector writes are atomic (each write less or equal to 4K will be
  * fully written to disk or not, FS checksum failure is a hardware bug, not
  * a result of the partial sector write).
  */
class MetaVolume {
    std::shared_ptr<VolumeRegistry>  meta_;
    size_t                           file_size_;
    mutable std::vector<u8>          double_write_buffer_;
    const std::string                path_;

    MetaVolume(std::shared_ptr<VolumeRegistry> meta);

public:

    /** Open existing meta-volume.
      * @param path Path to meta-volume.
      * @throw std::runtime_error on error.
      * @return new MetaVolume instance.
      */
    static std::unique_ptr<MetaVolume> open_existing(std::shared_ptr<VolumeRegistry> meta);

    // Accessors

    //! Get number of blocks in the volume.
    std::tuple<aku_Status, u32> get_nblocks(u32 id) const;

    //! Get total capacity of the volume.
    std::tuple<aku_Status, u32> get_capacity(u32 id) const;

    //! Get volume's generation.
    std::tuple<aku_Status, u32> get_generation(u32 id) const;

    size_t get_nvolumes() const;

    // Mutators

    /**
     * @brief Adds new tracked volume
     * @param id is a new volume's id
     * @param vol_capacity is a volume's capacity
     * @return status
     */
    aku_Status add_volume(u32 id, u32 vol_capacity, const std::string &path);

    aku_Status update(u32 id, u32 nblocks, u32 capacity, u32 gen);

    //! Set number of used blocks for the volume.
    aku_Status set_nblocks(u32 id, u32 nblocks);

    //! Set volume capacity
    aku_Status set_capacity(u32 id, u32 nblocks);

    //! Set generation
    aku_Status set_generation(u32 id, u32 nblocks);

    //! Flush entire file
    void flush();

    //! Flush one entry
    aku_Status flush(u32 id);
};


class Volume {
    AprPoolPtr  apr_pool_;
    AprFilePtr  apr_file_handle_;
    u32         file_size_;
    u32         write_pos_;
    std::string path_;
    // Optional mmap
    std::unique_ptr<MemoryMappedFile> mmap_;
    const u8* mmap_ptr_;

    Volume(const char* path, size_t write_pos);
    
public:
    /** Create new volume.
      * @param path Path to volume.
      * @param capacity Size of the volume in blocks.
      * @throw std::runtime_exception on error.
      */
    static void create_new(const char* path, size_t capacity);

    /** Open volume.
      * @throw std::runtime_error on error.
      * @param path Path to volume file.
      * @param pos Write position inside volume (in blocks).
      * @return New instance of V2::Volume.
      */
    static std::unique_ptr<Volume> open_existing(const char* path, size_t pos);

    // Mutators

    void reset();

    //! Append block to file (source size should be 4 at least BLOCK_SIZE)
    std::tuple<aku_Status, BlockAddr> append_block(const u8* source);

    std::tuple<aku_Status, BlockAddr> append_block(const IOVecBlock* source);

    //! Flush volume
    void flush();

    // Accessors

    //! Read fixed size block from file
    aku_Status read_block(u32 ix, u8* dest) const;

    //! Read fixed size block from file
    std::tuple<aku_Status, std::unique_ptr<IOVecBlock>> read_block(u32 ix) const;

    /**
     * @brief Read block without copying the data (only works if mmap available)
     * @param ix is an index of the page
     * @return status (AKU_EUNAVAILABLE if mmap is not present and the caller should use `read_block` instead)
     */
    std::tuple<aku_Status, const u8*> read_block_zero_copy(u32 ix) const;

    //! Return size in blocks
    u32 get_size() const;

    //! Return path of volume
    std::string get_path() const;
};

}  // namespace V2
}  // namespace Akumuli
