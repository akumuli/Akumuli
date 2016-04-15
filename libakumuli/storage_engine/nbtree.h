#pragma once
#include "blockstore.h"
#include "compression.h"

namespace Akumuli {
namespace V2 {

/** Necklace B-tree data-structure implementation.
  * Outline:
  *
  *
  *                                                   [superblock0]
  *                                                         |
  *              +------------------------------+---....----+----~
  *              |                              |
  *              v                              v
  *        [superblock0]<-----------------[superblock1]<--....
  *              |                              |
  *     +--------+---------+          +---------+---------+
  *     |        |         |          |         |         |
  *     v        v         v          v         v         v
  * [leaaf0]<--[....]<--[leafK]<--[leafK+1]<--[....]<--[leaf2K]<--~
  *
  * K is a fan-out range (Akumuli uses K=64).
  *
  * NBTree don't have one single root. Instead of that tree height is limited and
  * all nodes on one level are linked in backward direction (new node has pointer
  * to previous). Useful data stored only in leaf nodes.
  * Important property: superblock at level N are linked directly (using links to
  * underlying nodes only) to K^N nodes. All nodes a of the same size and all such
  * subtrees are full trees so space taken by each subtree are the same.
  * In this implementation nodes are stored in underlying block store. In this block
  * store old pages can be deleted to reclaim space. This process shouldn't corrupt
  * NBTree because only last node from each hierarchy level is needed to traverse
  * and append new data.
  *
  * Append.
  * - Append data to the current leaf block in main-memory.
  * - If block becomes full - write it to block-store. Add pointer to previous leaf
  *   node to the current leaf node.
  * - Add link to newly saved block to the current superblock on level 1.
  * - If superblock on level 1 become full - write it to block-store. Add pointer to
  *   previous superblock on level 1.
  * - Add link to  newly saved block to the current superblock on level 2, etc.
  *
  * Application should store somewhere root of the NBTree (the rightmost superblock in
  * the top layer).
  *
  * Application should  maintain metadata inside each superblock. Each node link should
  * contain the following information about pointee: version, tree level, number of
  * elements in the subtree, series id, smallest/largest timestamp of the subtree,
  * address of the node, smallest/largest value of the subtree, sum of the elements of
  * the subtree. This information can be used to speedup some aggregation queries, like
  * count(), avg(), sum() etc.
  */


/** NBTree leaf node. Supports append operation.
  * Can be commited to block store when full.
  */
class NBTreeNode {
    std::shared_ptr<BlockStore> bstore_;
    //! Series id
    aku_ParamId id_;
    //! Root address
    LogicAddr prev_;
    //! Buffer for pending updates
    std::vector<uint8_t> buffer_;
    //! DataBlockWriter for pending `append` operations.
    DataBlockWriter writer_;
public:

    /** C-tor.
      * @param id Series id.
      * @param link to block store.
      * @param prev Prev element of the tree.
      */
    NBTreeNode(aku_ParamId id, std::shared_ptr<BlockStore> bstore, LogicAddr prev);

    //! Append values to NBTree
    aku_Status append(aku_Timestamp ts, double value);

    /** Flush all pending changes to block store and close.
      * Calling this function too often can result in unoptimal space usage.
      */
    aku_Status commit();
};


/** This object represents block store backed tree.
  * It contains data from one time-series.
  * This data-structure supports only append operation but
  * other operations (delete/insert) can be implemented if
  * needed.
  */
class NBTreeNode {

};



}}  // namespaces
