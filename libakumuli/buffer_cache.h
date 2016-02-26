#pragma once

#include "compression.h"

#include <list>
#include <map>
#include <memory>
#include <mutex>

namespace Akumuli {

struct ChunkCache {
    //! Volume id + entry index
    typedef std::tuple<int, int>     KeyT;
    typedef std::tuple<KeyT, size_t> QueueItemT;
    typedef std::shared_ptr<UncompressedChunk> ItemT;

    std::map<KeyT, ItemT> cache_;
    std::list<QueueItemT> fifo_;
    size_t                total_size_;
    mutable std::mutex    mutex_;
    const size_t          size_limit_;

    ChunkCache(size_t limit);

    bool contains(KeyT key) const;

    ItemT get(KeyT key);

    void put(KeyT key, const std::shared_ptr<UncompressedChunk>& header);
};
}
