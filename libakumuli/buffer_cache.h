#pragma once

#include "compression.h"

#include <map>
#include <memory>

namespace Akumuli {

struct BufferCache
        // FIXME: naming BufferCache that caches ChunkHeaders that actually an uncompressed chunk representations
{
    //! Volume id + entry index
    typedef std::tuple<int, int> KeyT;

    std::map<KeyT, std::shared_ptr<ChunkHeader>> cache_;

    bool contains(KeyT key) const;

    bool try_get(KeyT key, ChunkHeader *dest);

    void save(KeyT key, std::unique_ptr<ChunkHeader>&& header);
};

}

