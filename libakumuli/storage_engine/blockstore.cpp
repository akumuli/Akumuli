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
    Block(std::shared_ptr<BlockStore> bs, LogicAddr addr, std::vector<uint8_t> data)
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
}

std::shared_ptr<BlockStore> BlockStore::open(std::string metapath, std::vector<std::string> volpaths) {
    return std::make_shared<BlockStore>(metapath, volpaths);
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
    auto status = volumes_[volix]->read_data(vol, dest.data());
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<Block>());
    }
    auto self = shared_from_this();
    auto block = std::make_shared(Block(self, addr, std::move(data)));
    return std::make_tuple(status, std::move(block));
}

}}  // namespace
