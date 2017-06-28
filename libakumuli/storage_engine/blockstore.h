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
#include "volumeregistry.h"
#include "volume.h"
#include <random>
#include <mutex>
#include <map>
#include <string>

namespace Akumuli {
namespace StorageEngine {

//! Address of the block inside storage
typedef u64 LogicAddr;

//! This value represents empty addr. It's too large to be used as a real block addr.
static const LogicAddr EMPTY_ADDR = std::numeric_limits<LogicAddr>::max();

class Block;

struct BlockCache {
    typedef std::shared_ptr<Block> PBlock;
    std::vector<PBlock> block_cache_;
    const u32 bits_;
    // RNG
    std::random_device dev_;
    std::mt19937 gen_;
    std::uniform_int_distribution<u32> dist_;

    /** Check status of the cache cell.
      * Return 0 if there is no such addr in the cache and slot is free.
      * Return 1 if addr is not present in the cache but slot is occupied by the other block.
      * Return 2 if addr is already present in the cache.
      */
    int probe(LogicAddr addr);

    BlockCache(u32 Nbits);

    void insert(PBlock block);

    PBlock loockup(LogicAddr addr);
};


struct BlockStoreStats {
    size_t block_size;
    size_t capacity;
    size_t nblocks;
};

typedef std::map<std::string, BlockStoreStats> PerVolumeStats;

/** Blockstore. Contains collection of volumes.
 * Translates logic adresses into physical ones.
 */

struct BlockStore {

    virtual ~BlockStore() = default;

    /** Read block from blockstore
      */
    virtual std::tuple<aku_Status, std::shared_ptr<Block>> read_block(LogicAddr addr) = 0;

    /** Add block to blockstore.
      * @param data Pointer to buffer.
      * @return Status and block's logic address.
      */
    virtual std::tuple<aku_Status, LogicAddr> append_block(std::shared_ptr<Block> data) = 0;

    //! Flush all pending changes.
    virtual void flush() = 0;

    //! Check if addr exists in block-store
    virtual bool exists(LogicAddr addr) const = 0;

    //! Compute checksum of the input data.
    virtual u32 checksum(u8 const* begin, size_t size) const = 0;

    virtual BlockStoreStats get_stats() const = 0;

    virtual PerVolumeStats get_volume_stats() const = 0;
};

class FileStorage : public BlockStore {
protected:
    //! Metadata volume.
    std::unique_ptr<MetaVolume> meta_;
    //! Array of volumes.
    std::vector<std::unique_ptr<Volume>> volumes_;
    //! "Dirty" flags.
    std::vector<int> dirty_;
    //! Current volume.
    u32 current_volume_;
    //! Current generation.
    u32 current_gen_;
    //! Size of the blockstore in blocks.
    size_t total_size_;
    //! Used to protect all internal state
    mutable std::mutex lock_;
    //! Volume names (for nice statistics)
    std::vector<std::string> volume_names_;

    //! Secret c-tor.
    FileStorage(std::shared_ptr<VolumeRegistry> meta);

    virtual void adjust_current_volume() = 0;
    void handle_volume_transition();

public:
    static void create(std::vector<std::tuple<u32, std::string>> vols);

    /** Add block to blockstore.
     * @param data Pointer to buffer.
     * @return Status and block's logic address.
     */
    virtual std::tuple<aku_Status, LogicAddr> append_block(std::shared_ptr<Block> data);

    virtual void flush();

    virtual u32 checksum(u8 const* data, size_t size) const;

    virtual BlockStoreStats get_stats() const;

    virtual PerVolumeStats get_volume_stats() const;
};

class FixedSizeFileStorage : public FileStorage,
                             public std::enable_shared_from_this<FixedSizeFileStorage> {
    //! Secret c-tor.
    FixedSizeFileStorage(std::shared_ptr<VolumeRegistry> meta);

protected:
    virtual void adjust_current_volume();

public:
    /** Create BlockStore instance (can be created only on heap).
      */
    static std::shared_ptr<FixedSizeFileStorage> open(std::shared_ptr<VolumeRegistry> meta);

    virtual bool exists(LogicAddr addr) const;

    /** Read block from blockstore
      */
    virtual std::tuple<aku_Status, std::shared_ptr<Block>> read_block(LogicAddr addr);
};

class ExpandableFileStorage : public FileStorage,
                              public std::enable_shared_from_this<ExpandableFileStorage> {
     std::string db_name_;

     //! Secret c-tor.
     ExpandableFileStorage(std::shared_ptr<VolumeRegistry> meta);

     std::unique_ptr<Volume> create_new_volume(u32 id);
protected:
     virtual void adjust_current_volume();

public:
     /**
      * Create BlockStore instance (can be created only on heap).
      * @param db_name is a logical database name
      * @param metapath is a place where the meta-page is located
      * @param volpaths is a list of volume paths
      * @param on_volume_advance is function object that gets called when new volume is created
      */
     static std::shared_ptr<ExpandableFileStorage> open(std::shared_ptr<VolumeRegistry> meta);

     virtual bool exists(LogicAddr addr) const;

     /** Read block from blockstore
      */
     virtual std::tuple<aku_Status, std::shared_ptr<Block>> read_block(LogicAddr addr);
};


//! Memory resident blockstore for tests (and machines with infinite RAM)
struct MemStore : BlockStore, std::enable_shared_from_this<MemStore> {
    std::vector<u8> buffer_;
    std::function<void(LogicAddr)> append_callback_;
    u32 write_pos_;
    u32 removed_pos_;
    u32 pad_;
    mutable std::mutex lock_;

    MemStore();

    MemStore(std::function<void(LogicAddr)> append_cb);

    virtual std::tuple<aku_Status, std::shared_ptr<Block> > read_block(LogicAddr addr);
    virtual std::tuple<aku_Status, LogicAddr> append_block(std::shared_ptr<Block> data);
    virtual void flush();
    virtual bool exists(LogicAddr addr) const;
    virtual u32 checksum(u8 const* data, size_t size) const;
    virtual BlockStoreStats get_stats() const;
    virtual PerVolumeStats get_volume_stats() const;
    void remove(size_t addr);
};


//! Represents memory block
class Block {
    std::vector<u8>           data_;
    LogicAddr                 addr_;
    const u8*                 zptr_;

public:
    Block(LogicAddr addr, std::vector<u8>&& data);

    //! This c-tor is used in zero-copy mechanism, ptr should outlive the Block object
    Block(LogicAddr addr, const u8* ptr);

    Block();

    bool is_readonly() const;

    const u8* get_data() const;

    const u8* get_cdata() const;

    u8* get_data();

    size_t get_size() const;

    LogicAddr get_addr() const;

    void set_addr(LogicAddr addr);
};

//! Should be used to create blockstore
struct BlockStoreBuilder {
    static std::shared_ptr<BlockStore> create_memstore();
    static std::shared_ptr<BlockStore> create_memstore(std::function<void(LogicAddr)> append_cb);
};

}
}  // namespace
