#pragma once
#include <cstdint>

extern "C" {

/** Library configuration.
 */
struct Config
{
    char* path_to_file;
    int32_t page_size;
    int32_t debug_mode;
};

}
