#pragma once

#include <iostream>
#include <string>
#include <stdint.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <memory>
#include <deque>
#include <sstream>

#include <lz4.h>
#include <roaring64map.hh>
#include <apr.h>
#include <apr_file_io.h>
#include <apr_general.h>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include "akumuli_def.h"

namespace Akumuli {

typedef std::unique_ptr<apr_pool_t, void (*)(apr_pool_t*)> AprPoolPtr;
typedef std::unique_ptr<apr_file_t, void (*)(apr_file_t*)> AprFilePtr;

struct LZ4Volume {
    std::string path_;
    enum {
        BLOCK_SIZE = 0x2000,
        FRAME_TUPLE_SIZE = sizeof(uint64_t)*3,
        NUM_TUPLES = (BLOCK_SIZE - sizeof(uint32_t)) / FRAME_TUPLE_SIZE,
    };
    union Frame {
        char block[BLOCK_SIZE];
        struct Partition {
            uint32_t size;
            uint64_t ids[NUM_TUPLES];
            uint64_t timestamps[NUM_TUPLES];
            double values[NUM_TUPLES];
        } part;
    } frames_[2];

    char buffer_[LZ4_COMPRESSBOUND(BLOCK_SIZE)];

    int pos_;
    LZ4_stream_t stream_;
    LZ4_streamDecode_t decode_stream_;
    AprPoolPtr pool_;
    AprFilePtr file_;
    size_t file_size_;
    const size_t max_file_size_;
    Roaring64Map bitmap_;
    const bool is_read_only_;
    int64_t bytes_to_read_;
    int elements_to_read_;  // in current frame

    void clear(int i);

    aku_Status write(int i);

    std::tuple<aku_Status, size_t> read(int i);

public:
    /**
     * @brief Create empty volume
     * @param file_name is string that contains volume file name
     * @param volume_size is a maximum allowed volume size
     */
    LZ4Volume(const char* file_name, size_t volume_size);

    /**
     * @brief Read existing volume
     * @param file_name volume file name
     */
    LZ4Volume(const char* file_name);

    ~LZ4Volume();

    void close();

    size_t file_size() const;

    aku_Status append(uint64_t id, uint64_t timestamp, double value);

    /**
     * @brief Read values in bulk (volume should be opened in read mode)
     * @param buffer_size is a size of any input buffer (all should be of the same size)
     * @param id is a pointer to buffer that should receive up to `buffer_size` ids
     * @param ts is a pointer to buffer that should receive `buffer_size` timestamps
     * @param xs is a pointer to buffer that should receive `buffer_size` values
     * @return number of elements being read or 0 if EOF reached or negative value on error
     */
    std::tuple<aku_Status, uint32_t> read_next(size_t buffer_size, uint64_t* id, uint64_t* ts, double* xs);

    const std::string get_path() const;

    void delete_file();

    const Roaring64Map& get_index() const;
};

class InputLog {
    typedef boost::filesystem::path Path;
    std::deque<std::unique_ptr<LZ4Volume>> volumes_;
    Path root_dir_;
    size_t volume_counter_;
    const size_t max_volumes_;
    const size_t volume_size_;
    std::vector<Path> available_volumes_;

    void find_volumes();

    void open_volumes();

    std::string get_volume_name();

    void add_volume(std::string path);

    void remove_last_volume();

public:
    /**
     * @brief Create writeable input log
     * @param rootdir is a directory containing all volumes
     * @param nvol max number of volumes
     * @param svol individual volume size
     */
    InputLog(const char* rootdir, size_t nvol, size_t svol);

    /**
     * @brief Recover information from input log
     * @param rootdir is a directory containing all volumes
     */
    InputLog(const char* rootdir);

    void reopen();

    /** Delete all files.
      */
    void delete_files();

    /** Append data point to the log.
      * Return true on oveflow. Parameter `stale_ids` will be filled with ids that will leave the
      * input log on next rotation. Rotation should be triggered manually.
      */
    aku_Status append(uint64_t id, uint64_t timestamp, double value, std::vector<uint64_t>* stale_ids);

    /**
     * @brief Read values in bulk (volume should be opened in read mode)
     * @param buffer_size is a size of any input buffer (all should be of the same size)
     * @param id is a pointer to buffer that should receive up to `buffer_size` ids
     * @param ts is a pointer to buffer that should receive `buffer_size` timestamps
     * @param xs is a pointer to buffer that should receive `buffer_size` values
     * @return number of elements being read or 0 if EOF reached or negative value on error
     */
    std::tuple<aku_Status, uint32_t> read_next(size_t buffer_size, uint64_t* id, uint64_t* ts, double* xs);

    void rotate();
};

}  // namespace
