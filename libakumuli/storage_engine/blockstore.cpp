#include "blockstore.h"
#include "log_iface.h"
#include "util.h"
#include "status_util.h"

namespace Akumuli {
namespace StorageEngine {


Block::Block(std::shared_ptr<BlockStore> bs, LogicAddr addr, std::vector<u8>&& data)
    : store_(bs)
    , data_(std::move(data))
    , addr_(addr)
{
}

const u8* Block::get_data() const {
    return data_.data();
}

size_t Block::get_size() const {
    return data_.size();
}


FixedSizeFileStorage::FixedSizeFileStorage(std::string metapath, std::vector<std::string> volpaths)
    : meta_(MetaVolume::open_existing(metapath.c_str()))
    , current_volume_(0)
    , current_gen_(0)
    , total_size_(0)
{
    for (u32 ix = 0ul; ix < volpaths.size(); ix++) {
        auto volpath = volpaths.at(ix);
        u32 nblocks = 0;
        aku_Status status = AKU_SUCCESS;
        std::tie(status, nblocks) = meta_->get_nblocks(ix);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, std::string("Can't open blockstore, volume " +
                                                   std::to_string(ix) + " failure: " +
                                                   StatusUtil::str(status)));
            AKU_PANIC("Can't open blockstore - " + StatusUtil::str(status));
        }
        auto uptr = Volume::open_existing(volpath.c_str(), nblocks);
        volumes_.push_back(std::move(uptr));
        dirty_.push_back(0);
    }

    for (const auto& vol: volumes_) {
        total_size_ += vol->get_size();
    }

    // set current volume, current volume is a first volume with free space available
    for (size_t i = 0u; i < volumes_.size(); i++) {
        u32 curr_gen, nblocks;
        aku_Status status;
        std::tie(status, curr_gen) = meta_->get_generation(i);
        if (status == AKU_SUCCESS) {
            std::tie(status, nblocks) = meta_->get_nblocks(i);
        }
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't find current volume, meta-volume corrupted, error: "
                        + StatusUtil::str(status));
            AKU_PANIC("Meta-volume corrupted, " + StatusUtil::str(status));
        }
        if (volumes_[i]->get_size() > nblocks) {
            // Free space available
            current_volume_ = i;
            current_gen_ = curr_gen;
            break;
        }
    }
}

std::shared_ptr<FixedSizeFileStorage> FixedSizeFileStorage::open(std::string metapath, std::vector<std::string> volpaths) {
    auto bs = new FixedSizeFileStorage(metapath, volpaths);
    return std::shared_ptr<FixedSizeFileStorage>(bs);
}

void FixedSizeFileStorage::create(std::string metapath,
                                  std::vector<std::tuple<u32, std::string>> vols)
{
    std::vector<u32> caps;
    for (auto cp: vols) {
        std::string path;
        u32 capacity;
        std::tie(capacity, path) = cp;
        Volume::create_new(path.c_str(), capacity);
        caps.push_back(capacity);
    }
    MetaVolume::create_new(metapath.c_str(), caps.size(), caps.data());
}

static u32 extract_gen(LogicAddr addr) {
    return addr >> 32;
}

static BlockAddr extract_vol(LogicAddr addr) {
    return addr & 0xFFFFFFFF;
}

static LogicAddr make_logic(u32 gen, BlockAddr addr) {
    return static_cast<u64>(gen) << 32 | addr;
}

bool FixedSizeFileStorage::exists(LogicAddr addr) const {
    auto gen = extract_gen(addr);
    auto vol = extract_vol(addr);
    auto volix = gen % volumes_.size();
    aku_Status status;
    u32 actual_gen;
    std::tie(status, actual_gen) = meta_->get_generation(volix);
    if (status != AKU_SUCCESS) {
        return false;
    }
    u32 nblocks;
    std::tie(status, nblocks) = meta_->get_nblocks(volix);
    if (status != AKU_SUCCESS) {
        return false;
    }
    return actual_gen == gen && vol < nblocks;
}

std::tuple<aku_Status, std::shared_ptr<Block>> FixedSizeFileStorage::read_block(LogicAddr addr) {
    aku_Status status;
    auto gen = extract_gen(addr);
    auto vol = extract_vol(addr);
    auto volix = gen % volumes_.size();
    u32 actual_gen;
    u32 nblocks;
    std::tie(status, actual_gen) = meta_->get_generation(volix);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(AKU_EBAD_ARG, std::unique_ptr<Block>());
    }
    std::tie(status, nblocks) = meta_->get_nblocks(volix);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(AKU_EBAD_ARG, std::unique_ptr<Block>());
    }
    if (actual_gen != gen || vol >= nblocks) {
        return std::make_tuple(AKU_EBAD_ARG, std::unique_ptr<Block>());
    }
    std::vector<u8> dest(AKU_BLOCK_SIZE, 0);
    status = volumes_[volix]->read_block(vol, dest.data());
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, std::unique_ptr<Block>());
    }
    auto self = shared_from_this();
    auto block = std::make_shared<Block>(self, addr, std::move(dest));
    return std::make_tuple(status, std::move(block));
}

void FixedSizeFileStorage::advance_volume() {
    Logger::msg(AKU_LOG_INFO, "Advance volume called, current gen:" + std::to_string(current_gen_));
    current_volume_ = (current_volume_ + 1) % volumes_.size();
    aku_Status status;
    std::tie(status, current_gen_) = meta_->get_generation(current_volume_);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read generation of next volume, " + StatusUtil::str(status));
        AKU_PANIC("Can't read generation of the next volume, " + StatusUtil::str(status));
    }
    // If volume is not empty - reset it and change generation
    u32 nblocks;
    std::tie(status, nblocks) = meta_->get_nblocks(current_volume_);
    if (status != AKU_SUCCESS) {
        Logger::msg(AKU_LOG_ERROR, "Can't read nblocks of next volume, " + StatusUtil::str(status));
        AKU_PANIC("Can't read nblocks of the next volume, " + StatusUtil::str(status));
    }
    if (nblocks != 0) {
        current_gen_ += volumes_.size();
        auto status = meta_->set_generation(current_volume_, current_gen_);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't set generation on volume, " + StatusUtil::str(status));
            AKU_PANIC("Invalid BlockStore state, can't reset volume's generation, " + StatusUtil::str(status));
        }
        // Rest selected volume
        status = meta_->set_nblocks(current_volume_, 0);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, "Can't reset nblocks on volume, " + StatusUtil::str(status));
            AKU_PANIC("Invalid BlockStore state, can't reset volume's nblocks, " + StatusUtil::str(status));
        }
        volumes_[current_volume_]->reset();
        dirty_[current_volume_]++;
    }
}

std::tuple<aku_Status, LogicAddr> FixedSizeFileStorage::append_block(u8 const* data) {
    BlockAddr block_addr;
    aku_Status status;
    std::tie(status, block_addr) = volumes_[current_volume_]->append_block(data);
    if (status == AKU_EOVERFLOW) {
        // Move to next generation
        advance_volume();
        std::tie(status, block_addr) = volumes_.at(current_volume_)->append_block(data);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, 0ull);
        }
    }
    status = meta_->set_nblocks(current_volume_, block_addr + 1);
    if (status != AKU_SUCCESS) {
        AKU_PANIC("Invalid BlockStore state, " + StatusUtil::str(status));
    }
    dirty_[current_volume_]++;
    return std::make_tuple(status, make_logic(current_gen_, block_addr));
}

void FixedSizeFileStorage::flush() {
    for (size_t ix = 0; ix < dirty_.size(); ix++) {
        if (dirty_[ix]) {
            dirty_[ix] = 0;
            volumes_[ix]->flush();
        }
    }
    meta_->flush();
}


//! Memory resident blockstore for tests (and machines with infinite RAM)
struct MemStore : BlockStore, std::enable_shared_from_this<MemStore> {
    std::vector<u8> buffer_;
    u32 write_pos_;
    MemStore()
        : write_pos_(0)
    {
    }

    virtual std::tuple<aku_Status, std::shared_ptr<Block>> read_block(LogicAddr addr) {
        std::shared_ptr<Block> block;
        u32 offset = static_cast<u32>(AKU_BLOCK_SIZE * addr);
        if (buffer_.size() < (offset + AKU_BLOCK_SIZE)) {
            return std::make_tuple(AKU_EOVERFLOW, block);
        }
        std::vector<u8> data;
        data.reserve(AKU_BLOCK_SIZE);
        auto begin = buffer_.begin() + offset;
        auto end = begin + AKU_BLOCK_SIZE;
        std::copy(begin, end, std::back_inserter(data));
        block.reset(new Block(shared_from_this(), addr, std::move(data)));
        return std::make_tuple(AKU_SUCCESS, block);
    }

    virtual std::tuple<aku_Status, LogicAddr> append_block(const u8 *data) {
        std::copy(data, data + AKU_BLOCK_SIZE, std::back_inserter(buffer_));
        return std::make_tuple(AKU_SUCCESS, write_pos_++);
    }

    virtual void flush() {
        // no-op
    }

    virtual bool exists(LogicAddr addr) const {
        return addr < write_pos_;
    }
};


std::shared_ptr<BlockStore> BlockStoreBuilder::create_memstore() {
    return std::make_shared<MemStore>();
}

}}  // namespace
