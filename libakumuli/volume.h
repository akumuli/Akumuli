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

namespace Akumuli {
namespace V2 {

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;

class Volume
{
    AprPoolPtr apr_pool_;
    AprFilePtr apr_file_handle_;
    size_t file_size_;
    size_t write_pos_;

    Volume(const char* path, size_t write_pos);
public:

    enum {
        BLOCK_SIZE=4096,
    };

    /** Create new volume.
      * @param path is a path to created file
      * @param size is a size of the created file
      * @throw std::runtime_exception
      */
    static void create_new(const char* path, size_t size);

    /** Open volume.
      * @throw std::runtime_error
      * @param path should point to volume path
      * @param pos should contain correct write position inside volume
      * @return new instance of V2::Volume.
      */
    static std::unique_ptr<Volume> open_existing(const char* path, size_t pos);

    // Mutators

    //! Append block to file (source size should be 4 at least BLOCK_SIZE)
    aku_Status append_block(const uint8_t* source);

    //! Flush volume
    void flush();

    // Accessors

    //! Read filxed size block from file
    aku_Status read_block(uint32_t ix, uint8_t* dest) const;
};

}  // namespace V2
}  // namespace Akumuli
