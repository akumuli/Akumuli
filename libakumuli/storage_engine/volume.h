#pragma once
// stdlib
#include <memory>
#include <cstdint>
#include <future>

// libraries
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

// project
#include "akumuli.h"
#include "util.h"

namespace Akumuli {
namespace V2 {

//! Address of the block inside volume (index of the block)
typedef uint32_t BlockAddr;

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;


/** Class that represents metadata volume.
  * MetaVolume is a file that contains some information
  * about each regullar volume - write position, generation, etc.
  *
  * Hardware asumptions. At this point we assume that disck sector size is
  * 4KB and sector writes are atomic (each write less or equal to 4K will be
  * fully written to disk or not, FS checksum failure is a hardware bug, not
  * a result of the partial sector write).
  */
class MetaVolume {
    MemoryMappedFile mmap_;
    size_t file_size_;
    uint8_t* mmap_ptr_;

    MetaVolume(const char* path);
public:

    enum {
        BLOCK_SIZE=4096,
    };

    /** Create new meta-volume.
      * @param path Path to created file.
      * @param capacity Size of the created file (in blocks).
      * @param vol_capacities Array of capacities of all volumes.
      * @throw std::runtime_exception
      */
    static void create_new(const char* path, size_t capacity, uint32_t const* vol_capacities);

    /** Open existing meta-volume.
      * @param path Path to meta-volume.
      * @throw std::runtime_error on error.
      * @return new MetaVolume instance.
      */
    static std::unique_ptr<MetaVolume> open_existing(const char* path);

    // Accessors

    //! Get number of blocks in the volume.
    std::tuple<aku_Status, uint32_t> get_nblocks(uint32_t id) const;

    //! Get total capacity of the volume.
    std::tuple<aku_Status, uint32_t> get_capacity(uint32_t id) const;

    //! Get volume's generation.
    std::tuple<aku_Status, uint32_t> get_generation(uint32_t id) const;

    // Mutators

    aku_Status update(uint32_t id, uint32_t nblocks, uint32_t capacity, uint32_t gen);

    //! Set number of used blocks for the volume.
    aku_Status set_nblocks(uint32_t id, uint32_t nblocks);

    //! Set volume capacity
    aku_Status set_capacity(uint32_t id, uint32_t nblocks);

    //! Set generation
    aku_Status set_generation(uint32_t id, uint32_t nblocks);

    //! Flush entire file
    void flush();

    //! Flush one entry
    aku_Status flush(uint32_t id);
};


class Volume
{
    AprPoolPtr apr_pool_;
    AprFilePtr apr_file_handle_;
    uint32_t file_size_;
    uint32_t write_pos_;

    Volume(const char* path, size_t write_pos);
public:

    enum {
        BLOCK_SIZE=4096,
    };

    /** Create new volume.
      * @param path Path to volume.
      * @param capacity Size of the volume in blocks.
      * @throw std::runtime_exception on error.
      */
    static void create_new(const char* path, size_t capacity);

    /** Open volume.
      * @throw std::runtime_error on error.
      * @param path Path to volume file.
      * @param pos Write position inside volume (in blocks).
      * @return New instance of V2::Volume.
      */
    static std::unique_ptr<Volume> open_existing(const char* path, size_t pos);

    // Mutators

    //! Append block to file (source size should be 4 at least BLOCK_SIZE)
    std::tuple<aku_Status, BlockAddr> append_block(const uint8_t* source);

    //! Flush volume
    void flush();

    // Accessors

    //! Read filxed size block from file
    aku_Status read_block(uint32_t ix, uint8_t* dest) const;
};

}  // namespace V2
}  // namespace Akumuli
