#pragma once
#include "volume.h"

namespace Akumuli {
namespace V2 {

//! Address of the block inside storage
typedef uint64_t LogicAddr;

class Block;

/** Blockstore. Contains collection of volumes.
  * Translates logic adresses into physical ones.
  */
class BlockStore : public std::enable_shared_from_this<BlockStore>
{
    std::unique_ptr<MetaVolume> meta_;
    std::vector<std::unique_ptr<Volume>> volumes_;
    std::vector<int> dirty_;
    uint32_t current_volume_;
    uint32_t current_gen_;

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
};

}}  // namespace
