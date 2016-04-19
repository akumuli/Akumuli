#pragma once
#include "volume.h"

namespace Akumuli {
namespace StorageEngine {

//! Address of the block inside storage
typedef uint64_t LogicAddr;

class Block;

/** Blockstore. Contains collection of volumes.
  * Translates logic adresses into physical ones.
  */
class BlockStore : public std::enable_shared_from_this<BlockStore>
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
    BlockStore(std::string metapath, std::vector<std::string> volpaths);

    void advance_volume();
public:
    /** Create BlockStore instance (can be created only on heap).
      */
    static std::shared_ptr<BlockStore> open(std::string metapath, std::vector<std::string> volpaths);

    /** Read block from blockstore
      */
    std::tuple<aku_Status, std::shared_ptr<Block> > read_block(LogicAddr addr);

    /** Add block to blockstore.
      * @param data Pointer to buffer.
      * @return Status and block's logic address.
      */
    std::tuple<aku_Status, LogicAddr> append_block(uint8_t const* data);

    void flush();

    bool exists(LogicAddr addr) const;

    // TODO: add static create fn
};

//! Represents memory block
class Block {
    std::weak_ptr<BlockStore> store_;
    std::vector<uint8_t> data_;
    LogicAddr addr_;
public:
    Block(std::shared_ptr<BlockStore> bs, LogicAddr addr, std::vector<uint8_t>&& data);

    const uint8_t* get_data() const;

    size_t get_size() const;
};

}}  // namespace
