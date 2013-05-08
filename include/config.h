#pragma once

extern "C" {

/** Library configuration.
 */
struct Config
{
    char* path_to_file;
    size_t page_size;
    bool debug_mode;
};

}
