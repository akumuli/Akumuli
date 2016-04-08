#pragma once
// stdlib
#include <memory>
#include <cstdint>
#include <future>

// libraries
#include <apr.h>
#include <apr_general.h>
#include <apr_file_io.h>

namespace Akumuli {
namespace V2 {

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;

class Volume
{
    AprPoolPtr apr_pool_;
    AprFilePtr apr_file_handle_;
    size_t file_size_;
    uint32_t pos_;

    Volume(const char* path);
public:

    enum {
        BLOCK_SIZE=4096,
    };

    /** Create new volume.
      * @throw std::runtime_exception
      */
    static void create_new(const char* path);

    /** Open volume.
      * @throw std::runtime_error
      * @return new instance of V2::Volume.
      */
    static std::unique_ptr<Volume> open_existing(const char* path);

    // Mutators

    //! Append block to file
    //void append_block(const uint8_t* source);
};

}  // namespace V2
}  // namespace Akumuli
