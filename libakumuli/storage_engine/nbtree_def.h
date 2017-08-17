#pragma once

/**
 * This header contains definitions that is shared between NB+tree and
 * query operators.
 */

#include "akumuli_def.h"  // for basic types
#include "blockstore.h"   // for LogicAddr

namespace Akumuli {
namespace StorageEngine {


enum class NBTreeBlockType : u16 {
    LEAF,   // data block
    INNER,  // super block
};


enum {
    AKU_NBTREE_FANOUT = 32,
    AKU_NBTREE_MAX_FANOUT_INDEX = 31,
};


/** Reference to tree node.
  * Ref contains some metadata: version, level, payload_size, id.
  * This metadata corresponds to the current node.
  * Also, reference contains some aggregates: count, begin, end, min, max, sum.
  * This aggregates corresponds to the current node if leve=0 (current node is a
  * leaf node) or to the pointee if level > 0. If level is 1 then pointee is a
  * leafa node and all this fields describes this leaf node. If level is 2 or more
  * then all this aggregates comes from entire subtree (e.g. min is a minimal value
  * in leaf nodes in pointee subtree).
  */
struct SubtreeRef {
    //! Number of elements in the subtree
    u64 count;
    //! Series Id
    aku_ParamId id;
    //! First element's timestamp
    aku_Timestamp begin;
    //! Last element's timestamp
    aku_Timestamp end;
    //! Object addr in blockstore
    LogicAddr addr;
    //! Smalles value
    double min;
    //! Registration time of the smallest value
    aku_Timestamp min_time;
    //! Largest value
    double max;
    //! Registration time of the largest value
    aku_Timestamp max_time;
    //! Summ of all elements in subtree
    double sum;
    //! First value in subtree
    double first;
    //! Last value in subtree
    double last;
    //! Node type
    NBTreeBlockType type;
    //! Node level in the tree
    u16 level;
    //! Payload size (real)
    u16 payload_size;
    //! Node version
    u16 version;
    //! Fan out index of the element (current)
    u16 fanout_index;
    //! Checksum of the block (not used for links to child nodes)
    u32 checksum;
} __attribute__((packed));

}} // namespace
