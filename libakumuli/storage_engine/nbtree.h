/**
 * Copyright (c) 2016 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/** Numeric B+tree data-structure implementation.
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
#include <deque>

// App headers
#include "nbtree_def.h"
#include "blockstore.h"
#include "compression.h"
#include "operators/operator.h"


namespace Akumuli {
namespace StorageEngine {


/** Describes how storage engine should process candlesticks
  * in corresponding query.
  */
struct NBTreeCandlestickHint {
    aku_Timestamp min_delta;
};


class NBTreeSuperblock;

/** NBTree leaf node. Supports append operation.
  * Can be commited to block store when full.
  */
class NBTreeLeaf {
    //! Root address
    LogicAddr prev_;
    //! Buffer for pending updates
    std::shared_ptr<Block> block_;
    //! DataBlockWriter for pending `append` operations.
    DataBlockWriter writer_;
    //! Fanout index
    u16 fanout_index_;

public:
    //! Empty tag to choose c-tor
    struct CloneTag {};

    enum class LeafLoadMethod {
        FULL_PAGE_LOAD,
        ONLY_HEADER,
    };

    //! Only for testing and benchmarks
    size_t _get_uncommitted_size() const;

    /** Create empty leaf node.
      * @param id Series id.
      * @param link to block store.
      * @param prev Prev element of the tree.
      * @param fanout_index Index inside current fanout
      */
    NBTreeLeaf(aku_ParamId id, LogicAddr prev, u16 fanout_index);

    /** Load from block store.
      * @param block Leaf's serialized data.
      * @param load Load method.
      * @note This c-tor panics if block is invalid or doesn't exists.
      */
    NBTreeLeaf(std::shared_ptr<Block> bstore);

    /**
     * @brief Clone leaf node
     * @param block is a pointer to block that contains leaf's data
     */
    NBTreeLeaf(std::shared_ptr<Block> block, CloneTag tag);

    /** Load from block store.
      * @param bstore Block store.
      * @param curr Address of the current leaf-node.
      * @param load Load method.
      */
    NBTreeLeaf(std::shared_ptr<BlockStore> bstore, LogicAddr curr);

    //! Get leaf metadata.
    SubtreeRef const* get_leafmeta() const;

    //! Returns number of elements.
    size_t nelements() const;

    //! Read timestamps
    std::tuple<aku_Timestamp, aku_Timestamp> get_timestamps() const;

    //! Get logic address of the previous node
    LogicAddr get_prev_addr() const;

    //! Set prev addr (works only on mutable node)
    void set_prev_addr(LogicAddr addr);

    //! Set fanout index of the node
    void set_node_fanout(u16 fanout);

    //! Return address of the node itself (or EMPTY_ADDR if not saved yet)
    LogicAddr get_addr() const;

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
    std::unique_ptr<RealValuedOperator> range(aku_Timestamp begin, aku_Timestamp end) const;

    std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin, aku_Timestamp end) const;

    //! Search for values in a range (in this and connected leaf nodes). DEPRICATED
    std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end, std::shared_ptr<BlockStore> bstore) const;

    //! Return iterator that returns candlesticks
    std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const;

    //! Group-aggregate query results iterator
    std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const;

    // Node split experiment //

    /**
     * @brief Split the node into the specified top node
     * @param bstore is a pointer to blockstore
     * @param pivot is a pivot point of the split
     * @param preserve_backrefs is a flag that controls the backrefs (ignored)
     * @param top_level is a top level node (the method will add links to this node
     *        instead of creating new inner node, the commit method of the `top_level` node wouldn't be called)
     * @return status and address of the new topmost node (always EMPTY_ADDR)
     */
    std::tuple<aku_Status, LogicAddr> split_into(std::shared_ptr<BlockStore> bstore,
                                                 aku_Timestamp pivot,
                                                 bool preserve_backrefs, u16 *fanout_index,
                                                 NBTreeSuperblock* top_level);


    /**
     * @brief Split the node
     * @param bstore is a pointer to blockstore
     * @param pivot is a pivot point of the split
     * @param preserve_backrefs is a flag that controls the backrefs (ignored)
     * @return status and address of the new topmost node
     */
    std::tuple<aku_Status, LogicAddr> split(std::shared_ptr<BlockStore> bstore,
                                            aku_Timestamp pivot,
                                            bool preserve_backrefs);
};


/** NBTree superblock. Stores refs to subtrees.
 */
class NBTreeSuperblock {
    std::shared_ptr<Block> block_;
    aku_ParamId            id_;
    u32                    write_pos_;
    u16                    fanout_index_;
    u16                    level_;
    LogicAddr              prev_;
    bool                   immutable_;

public:
    //! Create new writable node.
    NBTreeSuperblock(aku_ParamId id, LogicAddr prev, u16 fanout, u16 lvl);

    //! Read immutable node from block-store.
    NBTreeSuperblock(std::shared_ptr<Block> block);

    //! Read immutable node from block-store.
    NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore);

    //! Copy on write c-tor. Create new node, copy content referenced by address, remove last entery if needed.
    NBTreeSuperblock(LogicAddr addr, std::shared_ptr<BlockStore> bstore, bool remove_last);

    //! Append subtree ref
    aku_Status append(SubtreeRef const& p);

    //! Commit changes (even if node is not full)
    std::tuple<aku_Status, LogicAddr> commit(std::shared_ptr<BlockStore> bstore);

    //! Check if node is full (always true if node is immutable - c-tor #2)
    bool is_full() const;

    aku_Status read_all(std::vector<SubtreeRef>* refs) const;

    bool top(SubtreeRef* outref) const;

    bool top(LogicAddr* outaddr) const;

    //! Get node's level
    u16 get_level() const;

    //! Get fanout index of the node
    u16 get_fanout() const;

    SubtreeRef const* get_sblockmeta() const;

    size_t nelements() const;

    //! Return id of the tree
    aku_ParamId get_id() const;

    //! Return addr of the previous node
    LogicAddr get_prev_addr() const;

    //! Set previous addr for the node
    void set_prev_addr(LogicAddr addr);

    //! Change fanout index ouf the node
    void set_node_fanout(u16 newfanout);

    //! Return address of the node itself (or EMPTY_ADDR if not saved yet)
    LogicAddr get_addr() const;

    //! Read timestamps
    std::tuple<aku_Timestamp, aku_Timestamp> get_timestamps() const;

    std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end, std::shared_ptr<BlockStore> bstore) const;

    std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin,
                                                aku_Timestamp end,
                                                std::shared_ptr<BlockStore> bstore) const;

    std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end,
                                                   std::shared_ptr<BlockStore> bstore,
                                                   NBTreeCandlestickHint hint) const;

    //! Group-aggregate query results iterator
    std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin,
                                                      aku_Timestamp end,
                                                      u64 step, std::shared_ptr<BlockStore> bstore) const;

    // Node split experiment //
    /**
     * @brief Split the node (the results are copied to the provided node)
     * @param bstore is a link to backstore
     * @param pivot is a timestamp to pivot
     * @param preserve_horizontal_links is a flag that should be set to true to preserve the backrefs correctness (only
     *        needed for the topmost node)
     * @param root is a new root node (all content of this node will be copied there alongside the updated refs)
     * @return status, address of the last child (if preserve_horizontal_links was set to true)
     */
    std::tuple<aku_Status, LogicAddr> split_into(std::shared_ptr<BlockStore> bstore,
                                                 aku_Timestamp pivot,
                                                 bool preserve_horizontal_links,
                                                 NBTreeSuperblock *root);

    /**
     * @brief Split the node
     * @param bstore is a link to backstore
     * @param pivot is a timestamp to pivot
     * @param preserve_horizontal_links is a flag that should be set to true to preserve the backrefs correctness (only
     *        needed for the topmost node)
     * @return status, address of the current node (or empty if root was used), address of the last child (if preserve_horizontal_links
     *         was set to true)
     */
    std::tuple<aku_Status, LogicAddr, LogicAddr> split(std::shared_ptr<BlockStore> bstore,
                                                 aku_Timestamp pivot,
                                                 bool preserve_horizontal_links);
};


//! NBTree extent
struct NBTreeExtent {

    virtual ~NBTreeExtent() = default;

    enum class ExtentStatus {
        OK,
        NEW,
        KILLED_BY_RETENTION,
    };

    /**
     * @brief Check node status
     */
    virtual ExtentStatus status() const = 0;

    /** Append new data to the root (doesn't work with superblocks)
      * If new root created - return address of the previous root, otherwise return EMPTY
      */
    virtual std::tuple<bool, LogicAddr> append(aku_Timestamp ts, double value) = 0;

    /** Append subtree metadata to the root (doesn't work with leaf nodes)
      * If new root created - return address of the previous root, otherwise return EMPTY
      */
    virtual std::tuple<bool, LogicAddr> append(SubtreeRef const& pl) = 0;

    /** Write all changes to the block-store, even if node is not full.
      * @param final Should be set to false during normal operation and set to true during commit.
      * @return boolean value that is set to true when higher level node was saved as a
      *         result of the `commit` call and address of this node after commit.
      */
    virtual std::tuple<bool, LogicAddr> commit(bool final) = 0;

    //! Return iterator
    virtual std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end) const = 0;

    //! Returns true if extent was modified after last commit and has some unsaved data.
    virtual bool is_dirty() const = 0;

    //! Return iterator that will return single aggregated value.
    virtual std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin, aku_Timestamp end) const = 0;

    virtual std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const = 0;

    //! Return group-aggregate query results iterator
    virtual std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin, aku_Timestamp end, u64 step) const = 0;

    // Service functions //

    virtual void debug_dump(std::ostream& stream,
                            int base_indent,
                            std::function<std::string(aku_Timestamp)> tsformat,
                            u32 mask=0xFFFFFFFF) const = 0;

    //! Check extent's internal consitency
    static void check_extent(const NBTreeExtent *extent, std::shared_ptr<BlockStore> bstore, size_t level);

    // Node split //
    virtual std::tuple<bool, LogicAddr> split(aku_Timestamp ts) = 0;

    /**
     * @brief Updates address of the previous element (used by split)
     * @param addr is an address of the prev element for the newly created node
     * @return error code
     */
    virtual aku_Status update_prev_addr(LogicAddr addr) = 0;

    /**
     * @brief  Update current fanout index of the extent.
     * @note   Fanout of the extent is a position of the current extent in the next extent, this position can change
     *         due to split procedure. If split in the i+1 extent resulted in higher occupancy the i-th extent should
     *         get an increased fanout index.
     * @param  fanout_index is a new fanout index
     * @return status
     */
    virtual aku_Status update_fanout_index(u16 fanout_index) = 0;
};


enum class NBTreeAppendResult {
    OK,
    OK_FLUSH_NEEDED,
    FAIL_LATE_WRITE,
    FAIL_BAD_ID,
    FAIL_BAD_VALUE,
};

/** @brief This class represents set of roots of the NBTree.
  * It serves two purposes:
  * @li store all roots of the NBTree
  * @li create new roots lazily (NBTree starts with only one root and rarely goes above 2)
  */
class NBTreeExtentsList : public std::enable_shared_from_this<NBTreeExtentsList> {
    std::shared_ptr<BlockStore> bstore_;
    std::deque<std::unique_ptr<NBTreeExtent>> extents_;
    const aku_ParamId id_;
    //! Last timestamp
    aku_Timestamp last_;
    std::vector<LogicAddr> rescue_points_;
    bool initialized_;
    //! Number of write operations performed on object
    u64 write_count_;

    void open();
    void repair();
    void init();
    mutable RWLock lock_;

    // Testing
    std::random_device              rd_;
    std::mt19937                    rand_gen_;
    std::uniform_int_distribution<> dist_;
    const int                       threshold_;

    LogicAddr split_random_node(u32 ix);

    u32 chose_random_node();

    std::tuple<aku_Status, AggregationResult> get_aggregates(u32 ixnode) const;

    void check_rescue_points(u32 i) const;
public:

    std::tuple<aku_Status, LogicAddr> _split(aku_Timestamp pivot);

    /** C-tor
      * @param addresses List of root addresses in blockstore or list of resque points.
      * @param bstore Block-store.
      */
    NBTreeExtentsList(aku_ParamId id, std::vector<LogicAddr> addresses, std::shared_ptr<BlockStore> bstore);

    /** Append new subtree reference to extents list.
      * This operation can't fail and should be used only by NB-tree itself (from node-commit functions).
      * This property is not enforced by the typesystem.
      * Result is OK or OK_FLUSH_NEEDED (if rescue points list was changed).
      */
    bool append(SubtreeRef const& pl);

    /** Append new value to extents list.
      * This operation can fail if value is out of order.
      * On success result is OK or OK_FLUSH_NEEDED (if rescue points list was changed).
      */
    NBTreeAppendResult append(aku_Timestamp ts, double value);

    /**
     * @brief search function
     * @param begin is a start of the search interval
     * @param end is a next after the last element of the search interval
     * @return
     */
    std::unique_ptr<RealValuedOperator> search(aku_Timestamp begin, aku_Timestamp end) const;

    /**
     * @brief aggregate all values in search interval
     * @param begin is a start of the search interval
     * @param end is a next after the last element of the search interval
     * @return iterator that produces single value
     */
    std::unique_ptr<AggregateOperator> aggregate(aku_Timestamp begin, aku_Timestamp end) const;

    std::unique_ptr<AggregateOperator> candlesticks(aku_Timestamp begin, aku_Timestamp end, NBTreeCandlestickHint hint) const;

    /**
     * @brief Group values into buckets and return aggregate from each one of them
     * @param begin start of the search interval
     * @param end end of the search interval
     * @param step bucket size
     * @return iterator
     */
    std::unique_ptr<AggregateOperator> group_aggregate(aku_Timestamp begin, aku_Timestamp end, aku_Timestamp step) const;

    //! Commit changes to btree and close (do not call blockstore.flush), return list of addresses.
    std::vector<LogicAddr> close();

    //! Get roots of the tree
    std::vector<LogicAddr> get_roots() const;

    //! Get roots of the tree (only for internal use)
    std::vector<LogicAddr> _get_roots() const;

    //! Get size of the data stored in memory in compressed form (only for internal use)
    size_t _get_uncommitted_size() const;

    //! Get pointers to extents (for tests).
    std::vector<NBTreeExtent const*> get_extents() const;

    //! Force lazy initialization process.
    void force_init();

    bool is_initialized() const;

    enum class RepairStatus {
        OK,
        SKIP,
        REPAIR
    };

    //! Calculate repair status for each rescue point.
    static RepairStatus repair_status(const std::vector<LogicAddr> &rescue_points);

    // Debug

    //! Walk the tree from the root and print it to the stdout
    static void debug_print(LogicAddr root, std::shared_ptr<BlockStore> bstore, size_t depth = 0);
};

/**
 * @brief Initialize SubtreeRef by reading leaf node (addr field is not set)
 * @param leaf is a non-empty leaf node
 * @param out is an output parameter
 * @return status
 */
aku_Status init_subtree_from_leaf(const NBTreeLeaf& leaf, SubtreeRef& out);

/**
 * @brief Initialize SubtreeRef by reading subtree (addr field is not set)
 * @param leaf is a non-empty superblock
 * @param out is an output parameter
 * @return status
 */
aku_Status init_subtree_from_subtree(const NBTreeSuperblock& node, SubtreeRef& backref);

}
}  // namespaces

