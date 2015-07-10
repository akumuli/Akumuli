#pragma once

#include "compression.h"

#include <map>
#include <list>
#include <memory>
#include <mutex>

namespace Akumuli {

struct BufferCache
        // FIXME: naming BufferCache that caches ChunkHeaders that actually an uncompressed chunk representations
{
    //! Volume id + entry index
    typedef std::tuple<int, int> KeyT;
    typedef std::tuple<KeyT, size_t> QueueItemT;
    typedef std::shared_ptr<ChunkHeader> ItemT;

    std::map<KeyT, ItemT> cache_;
    std::list<QueueItemT> fifo_;
    size_t                total_size_;
    mutable std::mutex    mutex_;
    const size_t          size_limit_;

    BufferCache(size_t limit);

    bool contains(KeyT key) const;

    ItemT get(KeyT key);

    void put(KeyT key, const std::shared_ptr<ChunkHeader>& header);
};

}

