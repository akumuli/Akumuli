#pragma once
#include "blockstore.h"
#include "compression.h"

namespace Akumuli {
namespace V2 {


//! Reference to tree node
struct SubtreeRef {
    //! Node version
    uint16_t      version;
    //! Node level in the tree
    uint16_t      level;
    //! Number of elements in the subtree
    uint32_t      count;
    //! Series Id
    aku_ParamId   id;
    //! First element's timestamp
    aku_Timestamp begin;
    //! Last element's timestamp
    aku_Timestamp end;
    //! Object addr in blockstore
    LogicAddr     addr;
    //! Smalles value
    double        min;
    //! Largest value
    double        max;
    //! Summ of all elements in subtree
    double        sum;
};


//! Configuration options for
struct NBTreeConfiguration {
    
};

/** This object represents BlockStore backed tree.
  * It contains data from one time-series.
  */
class NBTree {
    std::shared_ptr<BlockStore> bstore_;
    //! Series id
    aku_ParamId id_;
    //! Root address
    LogicAddr root_;
public:

    NBTree(aku_ParamId id, std::shared_ptr<BlockStore> bstore, LogicAddr root);

    // TODO: backfill and delete functions

    aku_Status append(aku_Timestamp ts, double value);
};

}}  // namespaces
