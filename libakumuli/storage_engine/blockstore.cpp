#include "blockstore.h"
#include "log_iface.h"
#include "util.h"

namespace Akumuli {
namespace V2 {


//! Represents memory block
class Block {
    std::weak_ptr<BlockStore> store_;
    std::vector<uint8_t> data_;
    LogicAddr addr_;
public:
    Block(std::shared_ptr<BlockStore> bs, LogicAddr addr, std::vector<uint8_t>&& data)
        : store_(bs)
        , data_(std::move(data))
        , addr_(addr)
    {
    }

    const uint8_t* get_data() const {
        return data_.data();
    }

    size_t get_size() const {
        return data_.size();
    }
};


BlockStore::BlockStore(std::string metapath, std::vector<std::string> volpaths)
    : meta_(MetaVolume::open_existing(metapath.c_str()))
{
    for (uint32_t ix = 0ul; ix < volpaths.size(); ix++) {
        auto volpath = volpaths.at(ix);
        uint32_t nblocks = 0;
        aku_Status status = AKU_SUCCESS;
        std::tie(status, nblocks) = meta_->get_nblocks(ix);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, std::string("Can't open blockstore, volume " +
                                                   std::to_string(ix) + " failure: " +
                                                   aku_error_message(status)));
            AKU_PANIC("Can't open blockstore");
        }
        auto uptr = Volume::open_existing(volpath.c_str(), nblocks);
        volumes_.push_back(std::move(uptr));
    }

    // set current volume, current volume is a volume with minimal generation
    uint32_t minix = 0u;
    uint32_t mingen = 0u;
    for (size_t i = 0u; i < volumes_.size(); i++) {
        uint32_t curr_gen;
        aku_Status status;
        std::tie(status, curr_gen) = meta_->get_generation(i);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't find current volume, meta-volume corrupted");
            AKU_PANIC("Meta-volume corrupted");
        }
        if (mingen > curr_gen) {
            mingen = curr_gen;
            minix = i;
        }
    }
    current_volume_ = minix;
    current_gen_ = mingen;
}

std::shared_ptr<BlockStore> BlockStore::open(std::string metapath, std::vector<std::string> volpaths) {
    auto bs = new BlockStore(metapath, volpaths);
    return std::shared_ptr<BlockStore>(bs);
}

static uint32_t extract_gen(LogicAddr addr) {
    return addr >> 32;
}

static BlockAddr extract_vol(LogicAddr addr) {
    return addr & 0xFFFFFFFF;
}

std::tuple<aku_Status, std::shared_ptr<Block>> BlockStore::read_block(LogicAddr addr) {
    auto gen = extract_gen(addr);
    auto vol = extract_vol(addr);
    auto volix = gen % volumes_.size();
    std::vector<uint8_t> dest(Volume::BLOCK_SIZE, 0);
    auto status = volumes_[volix]->read_block(vol, dest.data());
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<Block>());
    }
    auto self = shared_from_this();
    auto block = std::make_shared<Block>(self, addr, std::move(dest));
    return std::make_tuple(status, std::move(block));
}

static LogicAddr make_logic(uint32_t gen, BlockAddr addr) {
    return static_cast<uint64_t>(gen) << 32 | addr;
}

void BlockStore::advance_volume() {
    Logger::msg(AKU_LOG_INFO, "Advance volume called, current gen:" + std::to_string(current_gen_));
    current_volume_ = (current_volume_ + 1) % volumes_.size();
    current_gen_++;
    auto status = meta_->set_generation(current_volume_, current_gen_);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't set generation on volume");
        AKU_PANIC("Invalid BlockStore state, can't reset volume's generation");
    }
    // Rest selected volume
    status = meta_->set_nblocks(current_volume_, 0);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't reset nblocks on volume");
        AKU_PANIC("Invalid BlockStore state, can't reset volume's nblocks");
    }
    meta_->flush();
}

std::tuple<aku_Status, LogicAddr> BlockStore::append_block(uint8_t const* data) {
    BlockAddr block_addr;
    aku_Status status;
    std::tie(status, block_addr) = volumes_.at(current_volume_)->append_block(data);
    if (status == AKU_EOVERFLOW) {
        // Move to next generation
        advance_volume();
        std::tie(status, block_addr) = volumes_.at(current_volume_)->append_block(data);
        if (status != AKU_SUCCESS) {
            return std::make_pair(status, 0ull);
        }
    }
    volumes_[current_volume_]->flush();
    status = meta_->set_nblocks(current_volume_, block_addr + 1);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Invalid BlockStore state");
    }
    // TODO: don't flush too often
    meta_->flush();
    return std::make_tuple(status, make_logic(current_gen_, block_addr));
}

}}  // namespace
