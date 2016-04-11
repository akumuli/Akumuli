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

    BlockStore(std::string metapath, std::vector<std::string> volpaths);

private:

    static std::shared_ptr<BlockStore> open(std::string metapath, std::vector<std::string> volpaths);

    std::tuple<aku_Status, std::shared_ptr<Block> > read_block(LogicAddr addr);
};

}}  // namespace
