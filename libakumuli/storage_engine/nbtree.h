#pragma once
#include "blockstore.h"
#include "compression.h"

namespace Akumuli {
namespace StorageEngine {

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
  * [leaaf0]<--[....]<--[leafK]   [leafK+1]<--[....]<--[leaf2K]
  *
  * K is a fan-out range (Akumuli uses K=64).
  *
  * NBTree don't have one single root. Instead of that tree height is limited and
  * nodes on one level are linked in backward direction (new node has pointer
  * to previous). Useful data stored only in leaf nodes.
  *
  * Leaf nodes and superblocks from one subtree don't have links to previous subtree.
  * They can be connected only through upper level superblock that have links to all
  * existing subtrees.
  *
  * Important property: superblock at level N are linked directly (using links to
  * underlying nodes only) to K^N nodes. All nodes a of the same size and all such
  * subtrees are full trees so space taken by each subtree are the same (but there could
  * be some internal fragmentation though).
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
  * the top layer) and links to all nonfinished subtrees (these subtrees shouldn't be
  * connected to top superblock).
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
class NBTreeLeaf {
    std::shared_ptr<BlockStore> bstore_;
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
    NBTreeLeaf(aku_ParamId id, std::shared_ptr<BlockStore> bstore, LogicAddr prev);

    //! Append values to NBTree
    aku_Status append(aku_Timestamp ts, double value);

    /** Flush all pending changes to block store and close.
      * Calling this function too often can result in unoptimal space usage.
      */
    std::tuple<aku_Status, LogicAddr> commit();
};

class NBTree;

class NBTreeCursor {
public:
    NBTreeCursor(NBTree const& tree, aku_Timestamp start, aku_Timestamp stop);
    NBTreeCursor(NBTreeCursor const& other);
    NBTreeCursor(NBTreeCursor && other);
    NBTreeCursor& operator = (NBTreeCursor const& other);

    //! Returns number of elements in cursor
    size_t size();

    //! Return true if read operation is completed and elements stored in this cursor
    //! are the last ones.
    bool is_eof();

    //! Read element from cursor (not all elements can be loaded to cursor)
    std::tuple<aku_Status, aku_Timestamp, double> at(size_t ix);

    void proceed_next();
};

/** This object represents block store backed tree.
  * It contains data from one time-series.
  * This data-structure supports only append operation but
  * other operations (delete/insert) can be implemented if
  * needed.
  */
class NBTree {
    // NOTE: supernodes not implemented at this point so, generaly speaking,
    // database is a set of linked lists. Each one of those linked lists is
    // represented by NBTree instance and a set of NBTreeLeaf objects.

    //! Blockstore
    std::shared_ptr<BlockStore> bstore_;
    aku_ParamId id_;
    LogicAddr last_;
    std::unique_ptr<NBTreeLeaf> leaf_;

    //! leaf_ is guaranteed to be initialized after call to this method
    void reset_leaf();
public:
    /** C-tor
      * @param id Series id.
      * @param bstore Pointer to block-store.
      */
    NBTree(aku_ParamId id, std::shared_ptr<BlockStore> bstore);

    //! Append data-point to NBTree
    void append(aku_Timestamp ts, double value);

    //! Return list of roots starting from leaf node
    std::vector<LogicAddr> roots() const;

    /** Iterate through the tree.
      * If `start` is less then `stop` - iterate in forward direction,
      * if `start` is greater then the `stop` - iterate in backward direction.
      * Interval [start, stop) is semi-open.
      * @param start Timestamp of the starting point of the range.
      * @param stop Timestamp of the first point out of the range.
      */
    NBTreeCursor iter(aku_Timestamp start, aku_Timestamp stop);
};



}}  // namespaces
