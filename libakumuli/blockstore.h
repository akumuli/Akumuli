#pragma once
#include "volume.h"

namespace Akumuli {
namespace V2 {


/** Blockstore. Contains collection of volumes.
  * Translates logic adresses into physical ones.
  */
struct BlockStore
{
    std::unique_ptr<MetaVolume> meta_;
    std::vector<std::unique_ptr<Volume> volumes_;

    BlockStore(std::string metapath, std::vector<std::string> volpaths);
};

}}  // namespace
