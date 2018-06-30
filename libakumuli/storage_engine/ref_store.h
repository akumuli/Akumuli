#pragma once

#include "nbtree_def.h"
#include "compression.h"
#include <algorithm>

namespace Akumuli {
namespace StorageEngine {

//! Naive implementation of the ref-storage
template<class TreeT>
struct ConsolidatedRefStorage {

    std::vector<SubtreeRef> refs_;

    void append(const SubtreeRef& ref) {
        if (refs_.capacity() == refs_.size()) {
            // Grow buffer in small steps to prevent fragmentation
            const size_t grow_step = 1;
            refs_.reserve(refs_.capacity() + grow_step);
        }
        refs_.push_back(ref);
    }

    //! Return true if buffer can accomodate the value
    bool has_space(u16 level) const {
        return nelements(level) < AKU_NBTREE_FANOUT;
    }

    aku_Status loadFrom(const TreeT& sblock) {
        if (sblock.nelements() > refs_.capacity() - refs_.size()) {
            refs_.reserve(refs_.capacity() + sblock.nelements());
        }
        return sblock.read_all(&refs_);
    }

    aku_Status saveTo(TreeT* sblock) {
        aku_Status status = AKU_SUCCESS;
        for (const SubtreeRef& ref: refs_) {
            if (ref.level == sblock->get_level() - 1) {
                status = sblock->append(ref);
                if (status != AKU_SUCCESS) {
                    break;
                }
            }
        }
        return status;
    }

    int nelements(u16 level) const {
        auto res = std::count_if(refs_.begin(), refs_.end(), [level](const SubtreeRef& cur) {
            return cur.level == level;
        });
        return static_cast<int>(res);
    }

    //! Remove layer and free space
    void remove_level(u16 level) {
        auto pred = [level] (const SubtreeRef& cur) {
            return cur.level != level;
        };
        auto sz = std::count_if(refs_.begin(), refs_.end(), pred);
        std::vector<SubtreeRef> newrefs;
        newrefs.reserve(static_cast<size_t>(sz));
        std::copy_if(refs_.begin(), refs_.end(), std::back_inserter(newrefs), pred);
        std::swap(refs_, newrefs);
    }
};

struct SubtreeRefCompressor {

    /**
     * @brief Encode SubtreeRef into binary format
     * @param dest is a pointer to the destination buffer
     * @param dest_size is a size of the destination buffer
     * @param ref is a SubtreeRef struct to encode
     * @return pointer to the first unmodified element on success, 'dest' on error
     */
    static u8* encode_subtree_ref(u8* dest, size_t dest_size, const SubtreeRef& ref);

    /**
     * @brief Decode SubtreeRef into binary format
     * @param source is a pointer to the buffer with encoded data
     * @param source_size is a size of the buffer
     * @param ref is a pointer to SubtreeRef struct that should receive data
     * @return pointer to the next element of buffer on success, 'source' on error
     */
    static const u8* decode_subtree_ref(const u8* source, size_t source_size, SubtreeRef* ref);

    /**
     * @brief filter out all records of some level
     * @param source is a pointer to the buffer with encoded data
     * @param source_size is a size of the buffer
     * @param level2remove is a level to filter out
     * @param out is a vector that should receive the result
     */
    static void filter(const u8* source, size_t source_size, u16 level2remove, std::vector<u8> *out);
};

struct CompressedRefStorage {
    aku_ParamId     id_;
    u16             version_;
    std::vector<u8> buffer_;

    CompressedRefStorage(aku_ParamId id, u16 version);

    template<class Func>
    void iter(const Func& func) const {
        if (buffer_.empty()) {
            return;
        }
        const u8* begin = buffer_.data();
        const u8* end = begin + buffer_.size();
        while(begin < end) {
            SubtreeRef ref = {};
            auto pout = SubtreeRefCompressor::decode_subtree_ref(begin,
                                                                 static_cast<size_t>(end - begin),
                                                                 &ref);
            if (pout == begin) {
                return;
            }
            ref.id      = id_;
            ref.version = version_;
            begin       = pout;

            func(ref);
        }
    }

    void append(const SubtreeRef& ref) {
        assert(ref.id == id_);
        assert(ref.version == version_);
        const size_t stage_size = sizeof(SubtreeRef)*2;
        u8 stage[stage_size] = {};
        auto pout = SubtreeRefCompressor::encode_subtree_ref(stage, stage_size, ref);
        if (pout == stage) {
            AKU_PANIC("Insufficient space for subtree-ref");
        }
        if (buffer_.capacity() - buffer_.size() < 512) {
            buffer_.reserve(buffer_.capacity() + 512);
        }
        std::copy(stage, pout, std::back_inserter(buffer_));
    }

    void remove_level(u16 level) {
        std::vector<u8> newbuf;
        SubtreeRefCompressor::filter(buffer_.data(), buffer_.size(), level, &newbuf);
        std::swap(buffer_, newbuf);
    }
};

}
}
