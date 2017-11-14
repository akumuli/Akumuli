#pragma once

#include "akumuli_def.h"
#include "blockstore.h"
#include "volume.h"

/**
 * Write Ahead Log component.
 * Akumuli uses WAL to perform in-place modifications (since Copy-on-Write can be
 * constly). WAL contains two filesystem components:
 * a) Aligned storage contains database pages without any modifications.
 * b) Indirection vector is an append only log that contains WalRecord structures
 *    that contains address information for blocks in the aligned storage.
 */

namespace Akumuli {
namespace StorageEngine {

/**
 * @brief Write Ahead Log record
 *
 */
struct WalRecord {
    u64 seq_number_;       //< Operation priority
    u64 page_offset_;      //< Address of the block in the blockstore
    LogicAddr logic_addr_; //< Logic addr of the block inside the volume store
};

class AppendOnlyLog {
    AprPoolPtr  apr_pool_;
    AprFilePtr  apr_file_handle_;
    std::string file_name_;
public:
    /**
     * @brief AppendOnlyLog c-tor
     * @param file_name is a log file name
     * Doesn't creates or opens a new file.
     */
    AppendOnlyLog(const char* file_name);

    /**
     * @brief create or open the log file
     * Creates new file if file doesn't exists or opens it.
     * @return status
     */
    aku_Status create_or_open();

    /**
     * @brief append data to log
     * @param block is a pointer to block
     * @param size is a block size
     * @return operation status
     */
    aku_Status append(const char* block, size_t size);

};

}
}
