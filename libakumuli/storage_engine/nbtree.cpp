#include "nbtree.h"

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


NBTreeNode::NBTreeNode(aku_ParamId id, std::shared_ptr<BlockStore> bstore, LogicAddr prev)
    : bstore_(bstore)
    , id_(id)
    , prev_(prev)
    , buffer_(BLOCK_SIZE, 0)
    , writer_(id, buffer_.data(), BLOCK_SIZE)
{
}

}}
