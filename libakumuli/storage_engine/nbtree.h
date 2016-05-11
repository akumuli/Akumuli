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
    AKU_NBTREE_FANOUT = 32,
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
    //! Largest value
    double max;
    //! Summ of all elements in subtree
    double sum;
    //! Node version
    u16 version;
    //! Node level in the tree
    u16 level;
    //! Payload size (real)
    u16 payload_size;
    //! Fan out index of the element (current)
    u16 fanout_index;
} __attribute__((packed));


/** NBTree iterator.
  * @note all ranges is semi-open. This means that if we're
  *       reading data from A to B, iterator should return
  *       data in range [A, B), and B timestamp should be
  *       greater (or less if we're reading data in backward
  *       direction) then all timestamps that we've read before.
  */
struct NBTreeIterator {

    //! Iteration direction
    enum class Direction {
        FORWARD, BACKWARD,
    };

    //! D-tor
    virtual ~NBTreeIterator() = default;

    /** Read data from iterator.
      * @param destts Timestamps destination buffer. On success timestamps will be written here.
      * @param destval Values destination buffer.
      * @param size Size of the  destts and destval buffers (should be the same).
      * @return status and number of elements written to both buffers.
      */
    virtual std::tuple<aku_Status, size_t> read(aku_Timestamp* destts, double* destval, size_t size) = 0;

    virtual Direction get_direction() = 0;
};


/** NBTree leaf node. Supports append operation.
  * Can be commited to block store when full.
  */
class NBTreeLeaf {
    //! Root address
    LogicAddr prev_;
    //! Buffer for pending updates
    std::vector<u8> buffer_;
    //! DataBlockWriter for pending `append` operations.
    DataBlockWriter writer_;
    //! Fanout index
    u16 fanout_index_;

public:
    enum class LeafLoadMethod {
        FULL_PAGE_LOAD,
        ONLY_HEADER,
    };

    /** Create empty leaf node.
      * @param id Series id.
      * @param link to block store.
      * @param prev Prev element of the tree.
      * @param fanout_index Index inside current fanout
      */
    NBTreeLeaf(aku_ParamId id, LogicAddr prev, u16 fanout_index);

    /** Load from block store.
      * @param bstore Block store.
      * @param curr Address of the current leaf-node.
      * @param load Load method.
      */
    NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr,
               LeafLoadMethod load = LeafLoadMethod::FULL_PAGE_LOAD);

    //! Returns number of elements.
    size_t nelements() const;

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

    //! Return node's fanout index
    u16 get_fanout() const;

    //! Return id of the tree
    aku_ParamId get_id() const;

    //! Return iterator that outputs all values in time range that is stored in this leaf.
    std::unique_ptr<NBTreeIterator> range(aku_Timestamp begin, aku_Timestamp end) const;

    //! Search for values in a range (in this and connected leaf nodes).
    std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end, std::shared_ptr<BlockStore> bstore) const;
};


/** NBTree superblock. Stores refs to subtrees.
 */
class NBTreeSuperblock {
    std::vector<u8> buffer_;
    aku_ParamId     id_;
    u32             write_pos_;
    u16             fanout_index_;
    u16             level_;
    LogicAddr       prev_;
    bool            immutable_;

public:
    //! Create new writable node.
    NBTreeSuperblock(aku_ParamId id, LogicAddr prev, u16 fanout, u16 lvl);

    //! Create node from block-store (node is immutable).
    NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore);

    // TODO: COW c-tor

    //! Append subtree ref
    aku_Status append(SubtreeRef const& p);

    //! Commit changes (even if node is not full)
    std::tuple<aku_Status, LogicAddr> commit(std::shared_ptr<BlockStore> bstore);

    //! Check if node is full (always true if node is immutable - c-tor #2)
    bool is_full() const;

    aku_Status read_all(std::vector<SubtreeRef>* refs) const;

    //! Get node's level
    u16 get_level() const;

    //! Get fanout index of the node
    u16 get_fanout() const;

    //! Return id of the tree
    aku_ParamId get_id() const;

    //! Read timestamps
    std::tuple<aku_Timestamp, aku_Timestamp> get_timestamps() const;

    std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end, std::shared_ptr<BlockStore> bstore) const;
};


//! NBTree root (leaf or superblock)
struct NBTreeRoot {
    virtual ~NBTreeRoot() = default;
    //! Append new data to the root (doesn't work with superblocks)
    virtual void append(aku_Timestamp ts, double value) = 0;
    //! Append subtree metadata to the root (doesn't work with leaf nodes)
    virtual void append(SubtreeRef const& pl) = 0;
    //! Write all changes to the block-store, even if node is not full. Return root address.
    virtual LogicAddr commit() = 0;
    //! Return iterator
    virtual std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end) const = 0;
};


/** @brief This class represents set of roots of the NBTree.
  * It serves two purposes:
  * @li store all roots of the NBTree
  * @li create new roots lazily (NBTree starts with only one root and rarely goes above 2)
  */
class NBTreeRootsCollection : public std::enable_shared_from_this<NBTreeRootsCollection> {
    std::shared_ptr<BlockStore> bstore_;
    std::vector<std::unique_ptr<NBTreeRoot>> roots_;
    aku_ParamId id_;
    std::vector<LogicAddr> rootaddr_;
    bool initialized_;

    void init();
public:
    /** C-tor
      * @param addresses List of root addresses in blockstore.
      * @param bstore Block-store.
      */
    NBTreeRootsCollection(aku_ParamId id, std::vector<LogicAddr> addresses, std::shared_ptr<BlockStore> bstore);

    void append(SubtreeRef const& pl);

    void append(aku_Timestamp ts, double value);

    std::unique_ptr<NBTreeIterator> search(aku_Timestamp begin, aku_Timestamp end) const;

    //! Commit changes to btree (do not call blockstore.flush), return list of addresses.
    std::vector<LogicAddr> commit();
};


}
}  // namespaces
