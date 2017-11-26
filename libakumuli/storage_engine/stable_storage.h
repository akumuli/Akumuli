#pragma once

#include "blockstore.h"
#include "util.h"

namespace Akumuli {
namespace StorageEngine {

/**
 * @brief Internal component for the stable storage
 */
class StableStorageVolume {

    std::string path_;
    std::unique_ptr<MemoryMappedFile> mmap_;

    enum {
        VOLUME_SIZE=256*1024*1024,  // 256MB
    };

public:
    /**
     * @brief Create new volume instance
     * @param path to the volume on disk
     */
    StableStorageVolume(const char* path);

    /**
     * @brief Return true if volume file exists
     */
    bool exists() const;

    /**
     * @brief open existing volume
     * @return operation status
     */
    aku_Status open_existing();

    /**
     * @brief create volume
     * @return operation status
     */
    aku_Status create();

    char* get_writable_mem();
};

class StableStorage {
    // Storage can support up to 4294967296 series
    //
    // PageId is a combination of u16 VolumeId and u16 offset

    enum {
        BLOCK_SIZE=4096,
    };
public:
    typedef u16 VolumeId;
    typedef u16 Offset;
    typedef u32 PageId;
private:
    std::string location_;
    std::unordered_map<VolumeId, std::shared_ptr<StableStorageVolume>> volumes_;
public:
    StableStorage(const char* location);

    /**
     * @brief get block by id
     * @param id is a page id
     * @return memory mapped file segment
     */
    std::shared_ptr<Block> get_block(PageId id);
};

}
}
