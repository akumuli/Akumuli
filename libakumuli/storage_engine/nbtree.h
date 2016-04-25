/** Necklace B-tree data-structure implementation.
  * Outline:
  *
  *
  *                                                   [superblock1]
  *                                                         |
  *              +------------------------------+---....----+----~
  *              |                              |
  *              v                              v
  *        [superblock1]<-----------------[superblock2]<--....
  *              |                              |
  *     +--------+---------+          +---------+---------+
  *     |        |         |          |         |         |
  *     v        v         v          v         v         v
  * [leaaf1]<--[....]<--[leafK]   [leafK+1]<--[....]<--[leaf2K]
  *
  * K is a fan-out range (Akumuli uses K=32).
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

#pragma once
// C++ headers
#include <stack>

// App headers
#include "blockstore.h"
#include "compression.h"

namespace Akumuli {
namespace StorageEngine {

enum {
    AKU_NBTREE_FANOUT=32,
};

struct SubtreeRefPayload {
    //! Number of elements in the subtree
    uint64_t      count;
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
} __attribute__((packed));


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
struct SubtreeRef : SubtreeRefPayload {
    //! Node version
    uint16_t      version;
    //! Node level in the tree
    uint16_t      level;
    //! Payload size (real)
    uint16_t      payload_size;
    //! Fan out index of the element (current)
    uint16_t      fanout_index;
} __attribute__((packed));


/** NBTree leaf node. Supports append operation.
  * Can be commited to block store when full.
  */
class NBTreeLeaf {
    //! Root address
    LogicAddr prev_;
    //! Buffer for pending updates
    std::vector<uint8_t> buffer_;
    //! DataBlockWriter for pending `append` operations.
    DataBlockWriter writer_;
    //! Fanout index
    uint16_t fanout_index_;
public:
    enum class LeafLoadMethod {
        FULL_PAGE_LOAD, ONLY_HEADER,
    };

    /** Create empty leaf node.
      * @param id Series id.
      * @param link to block store.
      * @param prev Prev element of the tree.
      * @param fanout_index Index inside current fanout
      */
    NBTreeLeaf(aku_ParamId id, LogicAddr prev, uint16_t fanout_index);

    /** Load from block store.
      * @param bstore Block store.
      * @param curr Address of the current leaf-node.
      * @param load Load method.
      */
    NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr,
               LeafLoadMethod load=LeafLoadMethod::FULL_PAGE_LOAD);

    //! Returns number of elements.
    size_t nelements();

    //! Read timestamps
    std::tuple<aku_Timestamp, aku_Timestamp> get_timestamps() const;

    //! Get logic address of the previous node
    LogicAddr get_prev_addr() const;

    /** Read all elements from the leaf node.
      * @param timestamps Destination for timestamps.
      * @param values Destination for values.
      * @return status.
      */
    aku_Status read_all(std::vector<aku_Timestamp>* timestamps, std::vector<double>* values) const;

    //! Append values to NBTree
    aku_Status append(aku_Timestamp ts, double value);

    /** Flush all pending changes to block store and close.
      * Calling this function too often can result in unoptimal space usage.
      */
    std::tuple<aku_Status, LogicAddr> commit(std::shared_ptr<BlockStore> bstore);
};


/** NBTree superblock. Stores links to other nodes.
 */
class NBTreeSuperblock {
    std::vector<uint8_t> buffer_;
    aku_ParamId id_;
    uint32_t write_pos_;
public:
    NBTreeSuperblock(aku_ParamId id);

    aku_Status append(SubtreeRefPayload const& p);

    std::tuple<aku_Status, LogicAddr> commit(std::shared_ptr<BlockStore> bstore);

    bool is_full() const;
};

class NBTree;

class NBTreeCursor {
    NBTree const& tree_;
    aku_Timestamp start_;
    aku_Timestamp stop_;
    std::vector<LogicAddr> backpath_;
    bool eof_;
    int proceed_calls_;
    aku_ParamId id_;

    enum {
        // On average each 4KB page will contain less then 1024 elements.
        SPACE_RESERVE=1024,
    };
    std::vector<aku_Timestamp> ts_;
    std::vector<double>        value_;

    //! Load next page into memory
    aku_Status load_next_page();
public:
    NBTreeCursor(NBTree const& tree, aku_Timestamp start, aku_Timestamp stop);

    //! Returns number of elements in cursor
    size_t size();

    //! Return true if read operation is completed and elements stored in this cursor
    //! are the last ones.
    bool is_eof();

    //! Read element from cursor (not all elements can be loaded to cursor)
    std::tuple<aku_Status, aku_Timestamp, double> at(size_t ix);

    void proceed();
};


//! Fwd ref. Implemented in nbtree.cpp.
class NBTreeRoot;


/** @brief This class represents set of roots of the NBTree.
  * It serves two purposes:
  * @li store all roots of the NBTree
  * @li create new roots lazily (NBTree starts with only one root and rarely goes above 2)
  */
class NBTreeRootsCollection : std::enable_shared_from_this<NBTreeRootsCollection> {
    std::shared_ptr<BlockStore> bstore_;
public:
    //! Acquire tree root (removes root from collection)
    std::unique_ptr<NBTreeRoot> lease(uint32_t level);

    //! Release tree root.
    void release(std::unique_ptr<NBTreeRoot> root);
};

/** This object represents block store backed tree.
  * It contains data from one time-series.
  * This data-structure supports only append operation but
  * other operations (delete/insert) can be implemented if
  * needed.
  */
class NBTree {

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

    //! Return series id
    aku_ParamId get_id() const;

    //! Append data-point to NBTree
    void append(aku_Timestamp ts, double value);

    //! Return list of roots starting from leaf node
    std::vector<LogicAddr> roots() const;

    //! Load Leaf node from block-store
    std::unique_ptr<NBTreeLeaf> load(LogicAddr addr) const;

    /** Iterate through the tree.
      * If `start` is less then `stop` - iterate in forward direction,
      * if `start` is greater then the `stop` - iterate in backward direction.
      * Interval [start, stop) is semi-open.
      * @param start Timestamp of the starting point of the range.
      * @param stop Timestamp of the first point out of the range.
      */
    std::vector<LogicAddr> iter(aku_Timestamp start, aku_Timestamp stop) const;

    /** Read all elements from the not yet built leaf node.
      * @param timestamps Destination for timestamps.
      * @param values Destination for values.
      * @return status.
      */
    aku_Status read_all(std::vector<aku_Timestamp>* timestamps, std::vector<double>* values) const;
};



}}  // namespaces
