#pragma once
#include "volume.h"

namespace Akumuli {
namespace StorageEngine {

//! Address of the block inside storage
typedef uint64_t LogicAddr;

class Block;

struct BlockStore {

    ~BlockStore() = default;

    /** Read block from blockstore
      */
    virtual std::tuple<aku_Status, std::shared_ptr<Block> > read_block(LogicAddr addr) = 0;

    /** Add block to blockstore.
      * @param data Pointer to buffer.
      * @return Status and block's logic address.
      */
    virtual std::tuple<aku_Status, LogicAddr> append_block(uint8_t const* data) = 0;

    //! Flush all pending changes.
    virtual void flush() = 0;

    //! Check if addr exists in block-store
    virtual bool exists(LogicAddr addr) const = 0;
};

/** Blockstore. Contains collection of volumes.
  * Translates logic adresses into physical ones.
  */
class FixedSizeFileStorage : public BlockStore,
                             public std::enable_shared_from_this<FixedSizeFileStorage>
{
    //! Metadata volume.
    std::unique_ptr<MetaVolume> meta_;
    //! Array of volumes.
    std::vector<std::unique_ptr<Volume>> volumes_;
    //! "Dirty" flags.
    std::vector<int> dirty_;
    //! Current volume.
    uint32_t current_volume_;
    //! Current generation.
    uint32_t current_gen_;
    //! Size of the blockstore in blocks.
    size_t total_size_;

    //! Secret c-tor.
    FixedSizeFileStorage(std::string metapath, std::vector<std::string> volpaths);

    void advance_volume();
public:
    /** Create BlockStore instance (can be created only on heap).
      */
    static std::shared_ptr<FixedSizeFileStorage> open(std::string metapath, std::vector<std::string> volpaths);

    static void create(std::string metapath, std::vector<std::tuple<uint32_t, std::string>> vols);

    /** Read block from blockstore
      */
    virtual std::tuple<aku_Status, std::shared_ptr<Block> > read_block(LogicAddr addr);

    /** Add block to blockstore.
      * @param data Pointer to buffer.
      * @return Status and block's logic address.
      */
    virtual std::tuple<aku_Status, LogicAddr> append_block(uint8_t const* data);

    virtual void flush();

    virtual bool exists(LogicAddr addr) const;

    // TODO: add static create fn
};

//! Represents memory block
class Block {
    std::weak_ptr<FixedSizeFileStorage> store_;
    std::vector<uint8_t> data_;
    LogicAddr addr_;
public:
    Block(std::shared_ptr<FixedSizeFileStorage> bs, LogicAddr addr, std::vector<uint8_t>&& data);

    const uint8_t* get_data() const;

    size_t get_size() const;
};

}}  // namespace
