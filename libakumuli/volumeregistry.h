#pragma once

#include "akumuli_def.h"
#include <vector>
#include <string>

namespace Akumuli {

/**
 * @brief Volume manager interface
 */
struct VolumeRegistry {
    typedef struct {
        u32 id;
        std::string path;
        u32 version;
        u32 nblocks;
        u32 capacity;
        u32 generation;
    } VolumeDesc;

    /** Read list of volumes and their sequence numbers.
      * @throw std::runtime_error in a case of error
      */
    virtual std::vector<VolumeDesc> get_volumes() const = 0;

    /**
     * @brief Add NEW volume synchroniously
     * @param vol is a volume description
     */
    virtual void add_volume(const VolumeDesc& vol) = 0;


    /**
     * @brief Update volume metadata asynchronously
     * @param vol is a volume description
     */
    virtual void update_volume(const VolumeDesc& vol) = 0;

    /**
     * @brief Get name of the database
     * @return database name
     */
    virtual std::string get_dbname() = 0;
};

}
