#include "input_log.h"
#include "log_iface.h"
#include "status_util.h"
#include "util.h"
#include "roaring.hh"

#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>

namespace Akumuli {

const static u16 V1_MAGIC = 0x1;

LogSequencer::LogSequencer()
    : counter_{0}
{
}

u64 LogSequencer::next() {
    return counter_++;
}

static void panic_on_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        throw std::runtime_error(std::string(msg) + " " + error_message);
    }
}

static void log_apr_error(apr_status_t status, const char* msg) {
    if (status != APR_SUCCESS) {
        char error_message[0x100];
        apr_strerror(status, error_message, 0x100);
        Logger::msg(AKU_LOG_ERROR, std::string(msg) + " " + error_message);
    }
}

static void _close_apr_file(apr_file_t* file) {
    apr_file_close(file);
}

static AprPoolPtr _make_apr_pool() {
    apr_pool_t* mem_pool = NULL;
    apr_status_t status = apr_pool_create(&mem_pool, NULL);
    panic_on_error(status, "Can't create APR pool");
    AprPoolPtr pool(mem_pool, &apr_pool_destroy);
    return pool;
}

static AprFilePtr _open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_WRITE|APR_BINARY|APR_CREATE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return file;
}

static AprFilePtr _open_file_ro(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_BINARY, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return file;
}

static size_t _get_file_size(apr_file_t* file) {
    apr_finfo_t info;
    auto status = apr_file_info_get(&info, APR_FINFO_SIZE, file);
    panic_on_error(status, "Can't get file info");
    return static_cast<size_t>(info.size);
}

static std::tuple<aku_Status, size_t> _write_frame(AprFilePtr& file, u32 size, void* array) {
    size_t outsize = 0;
    iovec io[2] = {
        {&size, sizeof(u32)},
        {array, size},
    };
    apr_status_t status = apr_file_writev_full(file.get(), io, 2, &outsize);
    if (status != APR_SUCCESS) {
        log_apr_error(status, "Can't write frame");
        return std::make_tuple(AKU_EIO, 0u);
    }
    return std::make_tuple(AKU_SUCCESS, outsize);
}

static aku_Status _flush_file(AprFilePtr& file) {
    apr_status_t status = apr_file_flush(file.get());
    if (status != APR_SUCCESS) {
        log_apr_error(status, "Can't flush file");
        return AKU_EIO;
    }
    return AKU_SUCCESS;
}

static std::tuple<aku_Status, size_t> _read_frame(AprFilePtr& file, u32 array_size, void* array) {
    u32 size;
    size_t bytes_read = 0;
    apr_status_t status = apr_file_read_full(file.get(), &size, sizeof(size), &bytes_read);
    if (status != APR_SUCCESS) {
        log_apr_error(status, "Can't read frame header");
        return std::make_tuple(AKU_EIO, 0u);
    }
    status = apr_file_read_full(file.get(), array, std::min(array_size, size), &bytes_read);
    if (status != APR_SUCCESS) {
        log_apr_error(status, "Can't read frame body");
        return std::make_tuple(AKU_EIO, 0u);
    }
    return std::make_tuple(AKU_SUCCESS, bytes_read);
}


//           //
// LZ4Volume //
//           //

void LZ4Volume::clear(int i) {
    memset(&frames_[i], 0, BLOCK_SIZE);
}

aku_Status LZ4Volume::write(int i) {
    assert(!is_read_only_);
    Frame& frame = frames_[i];
    frame.data_points.magic = V1_MAGIC;
    frame.data_points.sequence_number = sequencer_->next();
    // Do write
    int out_bytes = LZ4_compress_fast_continue(&stream_,
                                               frame.block,
                                               buffer_,
                                               BLOCK_SIZE,
                                               sizeof(buffer_),
                                               1);
    if(out_bytes <= 0) {
        throw std::runtime_error("LZ4 error");
    }
    size_t size;
    aku_Status status;
    std::tie(status, size) = _write_frame(file_,
                                          static_cast<u32>(out_bytes),
                                          buffer_);
    if (status != AKU_SUCCESS) {
        return status;
    }
    file_size_ += size;
    return _flush_file(file_);
}

std::tuple<aku_Status, size_t> LZ4Volume::read(int i) {
    assert(is_read_only_);
    Frame& frame = frames_[i];
    // Read frame
    u32 frame_size;
    aku_Status status;
    std::tie(status, frame_size) = _read_frame(file_, sizeof(buffer_), buffer_);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, 0);
    }
    assert(frame_size <= sizeof(buffer_));
    int out_bytes = LZ4_decompress_safe_continue(&decode_stream_,
                                                 buffer_,
                                                 frame.block,
                                                 frame_size,
                                                 BLOCK_SIZE);
    if(out_bytes <= 0) {
        return std::make_tuple(AKU_EBAD_DATA, 0);
    }
    return std::make_tuple(AKU_SUCCESS, frame_size + sizeof(u32));
}

LZ4Volume::LZ4Volume(LogSequencer* sequencer, const char* file_name, size_t volume_size)
    : path_(file_name)
    , pos_(0)
    , pool_(_make_apr_pool())
    , file_(_open_file(file_name, pool_.get()))
    , file_size_(0)
    , max_file_size_(volume_size)
    , bitmap_(std::make_shared<Roaring64Map>())
    , is_read_only_(false)
    , bytes_to_read_(0)
    , elements_to_read_(0)
    , sequencer_(sequencer)
{
    Logger::msg(AKU_LOG_TRACE, std::string("Open LZ4 volume ") + file_name + " for logging");
    clear(0);
    clear(1);
    LZ4_resetStream(&stream_);
}

static void null_deleter(apr_file_t* f) {
    assert(f == nullptr);
}

LZ4Volume::LZ4Volume(const char* file_name)
    : path_(file_name)
    , pos_(1)
    , pool_(_make_apr_pool())
    , file_(nullptr, &null_deleter)
    , file_size_(0)
    , max_file_size_(0)
    , bitmap_(std::make_shared<Roaring64Map>())
    , is_read_only_(true)
    , bytes_to_read_(0)
    , elements_to_read_(0)
{
    Logger::msg(AKU_LOG_TRACE, std::string("Open LZ4 volume ") + file_name + " for reading");
    clear(0);
    clear(1);
    LZ4_setStreamDecode(&decode_stream_, NULL, 0);
}

LZ4Volume::~LZ4Volume() {
    if (file_) {
        close();
    }
}

void LZ4Volume::open_ro() {
    assert(!is_opened());
    assert(file_size_ == 0);
    file_ = _open_file_ro(path_.c_str(), pool_.get());
    file_size_ = _get_file_size(file_.get());
    bytes_to_read_ = static_cast<i64>(file_size_);
}

bool LZ4Volume::is_opened() const {
    return static_cast<bool>(file_);
}

size_t LZ4Volume::file_size() const {
    return file_size_;
}

void LZ4Volume::close() {
    if(!is_read_only_) {
        // Write unfinished frame if it contains any data.
        if (frames_[pos_].data_points.size != 0) {
            write(pos_);
        }
    }
    file_.reset();
}


aku_Status LZ4Volume::flush_current_frame(FrameType type) {
    auto status = write(pos_);
    if (status != AKU_SUCCESS) {
        return status;
    }
    pos_ = (pos_ + 1) % 2;
    clear(pos_);
    Frame& frame = frames_[pos_];
    frame.header.frame_type = type;
    return AKU_SUCCESS;
}

aku_Status LZ4Volume::require_frame_type(FrameType type) {
    Frame& frame = frames_[pos_];
    if (frame.header.frame_type == FrameType::EMPTY) {
        frame.header.frame_type = type;
    }
    else if (frame.header.frame_type != type) {
        return flush_current_frame(type);
    }
    return AKU_SUCCESS;
}

aku_Status LZ4Volume::append(u64 id, u64 timestamp, double value) {
    auto status = require_frame_type(FrameType::DATA_ENTRY);
    if (status != AKU_SUCCESS) {
        return status;
    }
    bitmap_->add(id);
    Frame& frame = frames_[pos_];
    frame.data_points.ids[frame.data_points.size] = id;
    frame.data_points.tss[frame.data_points.size] = timestamp;
    frame.data_points.xss[frame.data_points.size] = value;
    frame.data_points.size++;
    if (frame.data_points.size == NUM_TUPLES) {
        status = write(pos_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        pos_ = (pos_ + 1) % 2;
        clear(pos_);
    }
    if(file_size_ >= max_file_size_) {
        return AKU_EOVERFLOW;
    }
    return AKU_SUCCESS;
}

struct MutableEntry : LZ4Volume::Frame::FlexibleEntry {
    union Bits {
        u64 value;
        struct {
            u32 len;
            u32 off;
        } components;
    };

    /** Get write offset and free space left for the value.
      */
    std::tuple<u32, u32> get_space_and_offset() {
        Bits bits;
        // The write_offset and space_left values for the first added element
        u32 write_offset = 0;
        u32 space_left   = static_cast<u32>(sizeof(*this) - sizeof(u64)*2);

        if (size > 0) {
            auto index = static_cast<int>(size) - 1;
            bits.value = vector[-1 - index*2];
            write_offset = bits.components.off + bits.components.len;
            auto space_used = write_offset + size*sizeof(u64)*2 + sizeof(u64)*2;
            assert(space_left >= space_used);
            space_left -= space_used;
        }
        return std::make_tuple(write_offset, space_left);
    }

    /** Return true if the value of size len can be written
      * into the frame (there is enough space).
      */
    bool can_write(u32 len) {
        u32 write_offset;
        u32 space_left;
        std::tie(write_offset, space_left) = get_space_and_offset();
        return len <= space_left;
    }

    std::tuple<u64, std::string> read_string(int ix) const {
        Bits bits;
        bits.value = vector[-1 - ix*2];
        u64 id = vector[-2 - ix*2];
        std::string result(data + bits.components.off, data + bits.components.off + bits.components.len);
        return std::make_tuple(id, result);
    }

    std::tuple<u64, std::vector<u64>> read_array(int ix) const {
        Bits bits;
        bits.value = vector[-1 - ix*2];
        u64 id = vector[-2 - ix*2];
        std::vector<u64> result(reinterpret_cast<const u64*>(data + bits.components.off),
                                reinterpret_cast<const u64*>(data + bits.components.off + bits.components.len));
        return std::make_tuple(id, result);
    }

    /** Append new value to the frame.
      * Invariant: `can_write(len) == true`.
      */
    void append(u64 id, const char* sname, u32 len) {
        u32 write_offset;
        u32 space_left;
        std::tie(write_offset, space_left) = get_space_and_offset();
        auto dest = data + write_offset;
        memcpy(dest, sname, len);
        int ix = size * -2;
        Bits bits;
        bits.components.len = len;
        bits.components.off = write_offset;
        vector[ix - 1] = bits.value;
        vector[ix - 2] = id;
        size++;
    }
};

aku_Status LZ4Volume::append_blob(FrameType type, u64 id, const char* payload, u32 len) {
    auto status = require_frame_type(type);
    if (status != AKU_SUCCESS) {
        return status;
    }
    MutableEntry* frame = reinterpret_cast<MutableEntry*>(&frames_[pos_].payload);
    if (!frame->can_write(len)) {
        status = flush_current_frame(type);
        if (status != AKU_SUCCESS) {
            return status;
        }
        frame = reinterpret_cast<MutableEntry*>(&frames_[pos_].payload);
    }

    // Invariant: len bytes can be writen into the frame
    frame->append(id, payload, len);

    static const u32 SIZE_THRESHOLD = 64;
    if (!frame->can_write(SIZE_THRESHOLD)) {
        status = flush_current_frame(type);
        if (status != AKU_SUCCESS) {
            return status;
        }
    }
    if(file_size_ >= max_file_size_) {
        return AKU_EOVERFLOW;
    }
    return AKU_SUCCESS;
}

aku_Status LZ4Volume::append(u64 id, const char* sname, u32 len) {
    return append_blob(FrameType::SNAME_ENTRY, id, sname, len);
}

aku_Status LZ4Volume::append(u64 id, const u64* recovery_array, u32 len) {
    return append_blob(FrameType::RECOVERY_ENTRY,
                       id,
                       reinterpret_cast<const char*>(recovery_array),
                       len*sizeof(u64));
}

std::tuple<aku_Status, u32> LZ4Volume::read_next(size_t buffer_size, u64* id, u64* ts, double* xs) {
    if (elements_to_read_ == 0) {
        if (bytes_to_read_ <= 0) {
            // Volume is finished
            return std::make_tuple(AKU_SUCCESS, 0);
        }
        pos_ = (pos_ + 1) % 2;
        clear(pos_);
        size_t bytes_read;
        aku_Status status;
        std::tie(status, bytes_read) = read(pos_);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, 0);
        }
        bytes_to_read_   -= bytes_read;
        elements_to_read_ = frames_[pos_].data_points.size;
    }
    Frame& frame = frames_[pos_];
    size_t nvalues = std::min(buffer_size, static_cast<size_t>(elements_to_read_));
    size_t frmsize = frame.data_points.size;
    for (size_t i = 0; i < nvalues; i++) {
        size_t ix = frmsize - elements_to_read_;
        id[i] = frame.data_points.ids[ix];
        ts[i] = frame.data_points.tss[ix];
        xs[i] = frame.data_points.xss[ix];
        elements_to_read_--;
    }
    return std::make_tuple(AKU_SUCCESS, static_cast<int>(nvalues));
}

std::tuple<aku_Status, u32> LZ4Volume::read_next(size_t buffer_size, InputLogRow* rows) {
    if (elements_to_read_ == 0) {
        if (bytes_to_read_ <= 0) {
            // Volume is finished
            return std::make_tuple(AKU_SUCCESS, 0);
        }
        pos_ = (pos_ + 1) % 2;
        clear(pos_);
        size_t bytes_read;
        aku_Status status;
        std::tie(status, bytes_read) = read(pos_);
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, 0);
        }
        bytes_to_read_   -= bytes_read;
        elements_to_read_ = frames_[pos_].data_points.size;
    }
    Frame& frame = frames_[pos_];
    size_t nvalues = std::min(buffer_size, static_cast<size_t>(elements_to_read_));
    size_t frmsize = frame.data_points.size;
    if (frame.header.frame_type == FrameType::DATA_ENTRY) {
        for (size_t i = 0; i < nvalues; i++) {
            size_t ix = frmsize - elements_to_read_;
            InputLogDataPoint data_point = {
                frame.data_points.tss[ix],
                frame.data_points.xss[ix]
            };
            rows[i].id = frame.data_points.ids[ix];
            rows[i].payload = data_point;
            elements_to_read_--;
        }
    }
    else if (frame.header.frame_type == FrameType::SNAME_ENTRY) {
        auto entry = reinterpret_cast<const MutableEntry*>(&frame.payload);
        for (size_t i = 0; i < nvalues; i++) {
            size_t ix = frmsize - elements_to_read_;
            InputLogSeriesName sname;
            assert(ix < frame.header.size);
            std::tie(rows[i].id, sname.value) = entry->read_string(ix);
            rows[i].payload = sname;
            elements_to_read_--;
        }
    }
    else if (frame.header.frame_type == FrameType::RECOVERY_ENTRY) {
        auto entry = reinterpret_cast<const MutableEntry*>(&frame.payload);
        for (size_t i = 0; i < nvalues; i++) {
            size_t ix = frmsize - elements_to_read_;
            InputLogRecoveryInfo recovery;
            assert(ix < frame.header.size);
            std::tie(rows[i].id, recovery.data) = entry->read_array(ix);
            rows[i].payload = recovery;
            elements_to_read_--;
        }
    }
    else {
        return std::make_tuple(AKU_EBAD_DATA, 0);
    }
    return std::make_tuple(AKU_SUCCESS, static_cast<int>(nvalues));
}

std::tuple<aku_Status, const LZ4Volume::Frame*> LZ4Volume::read_next_frame() {
    if (bytes_to_read_ <= 0) {
        // Volume is finished
        return std::make_tuple(AKU_SUCCESS, nullptr);
    }
    pos_ = (pos_ + 1) % 2;
    clear(pos_);
    size_t bytes_read;
    aku_Status status;
    std::tie(status, bytes_read) = read(pos_);
    if (status != AKU_SUCCESS) {
        return std::make_tuple(status, nullptr);
    }
    bytes_to_read_    -= bytes_read;
    elements_to_read_  = 0;  // assume all returned values are consumed
    const Frame* frame = &frames_[pos_];
    return std::make_tuple(AKU_SUCCESS, frame);
}

const std::string LZ4Volume::get_path() const {
    return path_;
}

void LZ4Volume::delete_file() {
    file_.reset();
    remove(path_.c_str());
}

const Roaring64Map& LZ4Volume::get_index() const {
    return *bitmap_;
}

aku_Status LZ4Volume::flush() {
    Frame& frame = frames_[pos_];
    if (frame.data_points.size != 0) {
        auto status = write(pos_);
        if (status != AKU_SUCCESS) {
            return status;
        }
        pos_ = (pos_ + 1) % 2;
        clear(pos_);
        if(file_size_ >= max_file_size_) {
            return AKU_EOVERFLOW;
        }
    }
    return AKU_SUCCESS;
}


//          //
// InputLog //
//          //

static std::tuple<bool, u32, u32> parse_filename(const std::string& name) {
    static const char* exp = "inputlog(\\d+)_(\\d+)\\.ils";
    static const boost::regex re(exp);
    boost::smatch smatch;
    if (boost::regex_search(name, smatch, re) && smatch.size() > 2) {
        try {
            auto volume_id = boost::lexical_cast<u32>(smatch[1].str());
            auto stream_id = boost::lexical_cast<u32>(smatch[2].str());
            return std::make_tuple(true, volume_id, stream_id);
        } catch (const boost::bad_lexical_cast&) {}
    }
    return std::make_tuple(false, 0, 0);
}

void InputLog::find_volumes() {
    if (!boost::filesystem::exists(root_dir_)) {
        throw std::runtime_error(root_dir_.string() + " doesn't exist");
    }
    if (!boost::filesystem::is_directory(root_dir_)) {
        throw std::runtime_error(root_dir_.string() + " is not a directory");
    }
    std::vector<std::tuple<u32, std::string>> volumes;
    for (auto it = boost::filesystem::directory_iterator(root_dir_);
         it != boost::filesystem::directory_iterator(); it++) {
        Path path = *it;
        bool is_volume;
        u32 volume_id;
        u32 stream_id;
        auto fn = path.filename().string();
        std::tie(is_volume, volume_id, stream_id) = parse_filename(fn);
        if (is_volume && stream_id == stream_id_) {
            auto abs_path = boost::filesystem::canonical(path, root_dir_).string();
            volumes.push_back(std::make_pair(volume_id, abs_path));
        }
    }
    std::sort(volumes.begin(), volumes.end());
    for (const auto& tup: volumes) {
        u32 volid;
        std::string name;
        std::tie(volid, name) = tup;
        available_volumes_.push_back(name);
    }
}

void InputLog::open_volumes() {
    for (const auto& path: available_volumes_) {
        std::unique_ptr<LZ4Volume> volume(new LZ4Volume(path.c_str()));
        volumes_.push_back(std::move(volume));
        volume_counter_++;
    }
    if (volumes_.empty() == false && volumes_.front()->is_opened() == false) {
        // Open the first volume to read from
        volumes_.front()->open_ro();
    }
}

std::string InputLog::get_volume_name() {
    std::stringstream filename;
    filename << "inputlog" << volume_counter_ << "_" << stream_id_ << ".ils";
    Path path = root_dir_ / filename.str();
    return path.string();
}

void InputLog::add_volume(std::string path) {
    if (boost::filesystem::exists(path)) {
        Logger::msg(AKU_LOG_INFO, std::string("Path ") + path + " already exists");
    }
    std::unique_ptr<LZ4Volume> volume(new LZ4Volume(sequencer_, path.c_str(), volume_size_));
    volumes_.push_front(std::move(volume));
    volume_counter_++;
}

void InputLog::remove_last_volume() {
    auto volume = std::move(volumes_.back());
    volumes_.pop_back();
    volume->delete_file();
    Logger::msg(AKU_LOG_INFO, std::string("Remove volume ") + volume->get_path());
}

InputLog::InputLog(LogSequencer* sequencer, const char* rootdir, size_t nvol, size_t svol, u32 stream_id)
    : root_dir_(rootdir)
    , volume_counter_(0)
    , max_volumes_(nvol)
    , volume_size_(svol)
    , stream_id_(stream_id)
    , sequencer_(sequencer)
{
    std::string path = get_volume_name();
    Logger::msg(AKU_LOG_INFO, std::string("Open input log ") + std::to_string(stream_id) + " for logging.");
    add_volume(path);
}

InputLog::InputLog(const char* rootdir, u32 stream_id)
    : root_dir_(rootdir)
    , volume_counter_(0)
    , max_volumes_(0)
    , volume_size_(0)
    , stream_id_(stream_id)
    , sequencer_(nullptr)
{
    Logger::msg(AKU_LOG_INFO, std::string("Open input log ") + std::to_string(stream_id) + " for recovery.");
    find_volumes();
    open_volumes();
}

void InputLog::reopen() {
    assert(volume_size_ == 0 &&  max_volumes_ == 0);  // read mode
    volumes_.clear();
    open_volumes();
}

void InputLog::delete_files() {
    Logger::msg(AKU_LOG_INFO, "Delete all volumes");
    for (auto& it: volumes_) {
        Logger::msg(AKU_LOG_INFO, std::string("Delete ") + it->get_path());
        it->delete_file();
    }
}

void InputLog::detect_stale_ids(std::vector<u64>* stale_ids) {
    // Extract stale ids
    assert(volumes_.size() > 0);
    std::vector<const Roaring64Map*> remaining;
    for (size_t i = 0; i < volumes_.size() - 1; i++) {
        // move from newer to older volumes
        remaining.push_back(&volumes_.at(i)->get_index());
    }
    Roaring64Map sum = Roaring64Map::fastunion(remaining.size(), remaining.data());
    auto stale = volumes_.back()->get_index() - sum;
    for (auto it = stale.begin(); it != stale.end(); it++) {
        stale_ids->push_back(*it);
    }
}

aku_Status InputLog::append(u64 id, u64 timestamp, double value, std::vector<u64>* stale_ids) {
    aku_Status result = volumes_.front()->append(id, timestamp, value);
    if (result == AKU_EOVERFLOW && volumes_.size() == max_volumes_) {
        detect_stale_ids(stale_ids);
    }
    return result;
}

aku_Status InputLog::append(u64 id, const char* sname, u32 len, std::vector<u64>* stale_ids) {
    aku_Status result = volumes_.front()->append(id, sname, len);
    if (result == AKU_EOVERFLOW && volumes_.size() == max_volumes_) {
        detect_stale_ids(stale_ids);
    }
    return result;
}

aku_Status InputLog::append(u64 id, const u64 *rescue_points, u32 len, std::vector<u64>* stale_ids) {
    aku_Status result = volumes_.front()->append(id, rescue_points, len);
    if (result == AKU_EOVERFLOW && volumes_.size() == max_volumes_) {
        detect_stale_ids(stale_ids);
    }
    return result;
}

std::tuple<aku_Status, u32> InputLog::read_next(size_t buffer_size, u64* id, u64* ts, double* xs) {
    while(true) {
        if (volumes_.empty()) {
            return std::make_tuple(AKU_SUCCESS, 0);
        }
        if (volumes_.front()->is_opened() == false) {
            volumes_.front()->open_ro();
        }
        aku_Status status;
        u32 result;
        std::tie(status, result) = volumes_.front()->read_next(buffer_size, id, ts, xs);
        if (result != 0) {
            return std::make_tuple(status, result);
        }
        volumes_.pop_front();
    }
}

std::tuple<aku_Status, u32> InputLog::read_next(size_t buffer_size, InputLogRow* rows) {
    while(true) {
        if (volumes_.empty()) {
            return std::make_tuple(AKU_SUCCESS, 0);
        }
        if (volumes_.front()->is_opened() == false) {
            volumes_.front()->open_ro();
        }
        aku_Status status;
        u32 result;
        std::tie(status, result) = volumes_.front()->read_next(buffer_size, rows);
        if (result != 0) {
            return std::make_tuple(status, result);
        }
        volumes_.pop_front();
    }
}

std::tuple<aku_Status, const LZ4Volume::Frame*> InputLog::read_next_frame() {
    while(true) {
        if (volumes_.empty()) {
            return std::make_tuple(AKU_ENO_DATA, nullptr);
        }
        if (volumes_.front()->is_opened() == false) {
            volumes_.front()->open_ro();
        }
        aku_Status status;
        const LZ4Volume::Frame* result;
        std::tie(status, result) = volumes_.front()->read_next_frame();
        if (result == nullptr && status == AKU_SUCCESS) {
            volumes_.pop_front();
            continue;
        }
        if (status != AKU_SUCCESS) {
            return std::make_tuple(status, nullptr);
        }
        return std::make_pair(AKU_SUCCESS, result);
    }
}

void InputLog::rotate() {
    if (volumes_.size() >= max_volumes_) {
        remove_last_volume();
    }
    std::string path = get_volume_name();
    add_volume(path);
    if (volumes_.size() > 1) {
        // Volume 0 is active, volume 1 should be closed
        volumes_.at(1)->close();
    }
}

aku_Status InputLog::flush(std::vector<u64>* stale_ids) {
    if (volumes_.empty()) {
        return AKU_SUCCESS;
    }
    aku_Status result = volumes_.front()->flush();
    if (result == AKU_EOVERFLOW && volumes_.size() == max_volumes_) {
        // Extract stale ids
        assert(volumes_.size() > 0);
        std::vector<const Roaring64Map*> remaining;
        for (size_t i = 0; i < volumes_.size() - 1; i++) {
            // move from newer to older volumes
            remaining.push_back(&volumes_.at(i)->get_index());
        }
        Roaring64Map sum = Roaring64Map::fastunion(remaining.size(), remaining.data());
        auto stale = volumes_.back()->get_index() - sum;
        for (auto it = stale.begin(); it != stale.end(); it++) {
            stale_ids->push_back(*it);
        }
    }
    return result;
}

//                 //
// ShardedInputLog //
//                 //

ShardedInputLog::ShardedInputLog(int concurrency,
                                 const char* rootdir,
                                 size_t nvol,
                                 size_t svol)
    : concurrency_(concurrency)
    , read_only_(false)
    , read_started_(false)
    , rootdir_(rootdir)
    , nvol_(nvol)
    , svol_(svol)
{
    streams_.resize(concurrency_);
}

std::tuple<aku_Status, int> get_concurrency_level(const char* root_dir) {
    // file name example: inputlog28_0.ils
    if (!boost::filesystem::exists(root_dir)) {
        return std::make_tuple(AKU_ENOT_FOUND, 0);
    }
    if (!boost::filesystem::is_directory(root_dir)) {
        return std::make_tuple(AKU_ENOT_FOUND, 0);
    }
    std::vector<std::tuple<u32, std::string>> volumes;
    u32 max_stream_id = 0;
    for (auto it = boost::filesystem::directory_iterator(root_dir);
         it != boost::filesystem::directory_iterator(); it++) {
        boost::filesystem::path path = *it;
        bool is_volume;
        u32 volume_id;
        u32 stream_id;
        auto fn = path.filename().string();
        std::tie(is_volume, volume_id, stream_id) = parse_filename(fn);
        max_stream_id = std::max(stream_id, max_stream_id);
    }
    return std::make_tuple(AKU_SUCCESS, static_cast<int>(max_stream_id + 1));  // stream ids are 0 based indexes
}

ShardedInputLog::ShardedInputLog(int concurrency,
                                 const char* rootdir)
    : concurrency_(concurrency)
    , read_only_(true)
    , read_started_(false)
    , rootdir_(rootdir)
    , nvol_(0)
    , svol_(0)
{
    if (concurrency_ == 0) {
        aku_Status status;
        Logger::msg(AKU_LOG_INFO, "Trying to retreive previous concurrency level");
        int newlevel;
        std::tie(status, newlevel) = get_concurrency_level(rootdir);
        if (status != AKU_SUCCESS) {
            Logger::msg(AKU_LOG_ERROR, std::string("Can't retreive concurrency level of the input log: ") +
                                                   StatusUtil::str(status));
        } else {
            Logger::msg(AKU_LOG_ERROR, std::string("Concurrency level of the input log is ") +
                                                   std::to_string(newlevel));
            concurrency_ = newlevel;
        }
    }
    for (int i = 0; i < concurrency_; i++) {
        std::unique_ptr<InputLog> log;
        log.reset(new InputLog(rootdir, i));
        streams_.push_back(std::move(log));
    }
}

InputLog& ShardedInputLog::get_shard(int i) {
    if (read_only_) {
        AKU_PANIC("Can't write read-only input log");
    }
    auto ix = i % streams_.size();
    if (!streams_.at(ix)) {
        std::unique_ptr<InputLog> log;
        log.reset(new InputLog(&sequencer_, rootdir_.c_str(), nvol_, svol_, static_cast<u32>(ix)));
        streams_.at(ix) = std::move(log);
    }
    return *streams_.at(ix);
}

void ShardedInputLog::init_read_buffers() {
    if (!read_only_) {
        AKU_PANIC("Can't read write-only input log");
    }
    assert(!read_started_);
    read_queue_.resize(concurrency_);
    for (size_t i = 0; i < read_queue_.size(); i++) {
        auto& str = streams_.at(i);
        auto  buf = &read_queue_.at(i);
        std::tie(buf->status, buf->frame) = str->read_next_frame();
        buf->pos = 0;
    }
    read_started_ = true;
    buffer_ix_ = -1;
}

int ShardedInputLog::choose_next() {
    size_t ixstart = 0;
    for (;ixstart < read_queue_.size(); ixstart++) {
        if (read_queue_.at(ixstart).status == AKU_SUCCESS &&
            read_queue_.at(ixstart).frame->data_points.size != 0) {
            break;
        }
    }
    if (ixstart == read_queue_.size()) {
        // Indicate that all buffers are bad.
        return -1;
    }
    int res = ixstart;
    for(size_t ix = ixstart + 1; ix < read_queue_.size(); ix++) {
        if (read_queue_.at(ix).status != AKU_SUCCESS ||
            read_queue_.at(ix).frame->data_points.size == 0) {
            // Current input log is done or errored, just skip it.
            continue;
        }
        if (read_queue_.at(ix).frame->data_points.sequence_number <
            read_queue_.at(res).frame->data_points.sequence_number) {
            res = ix;
        }
    }
    return res;
}

void ShardedInputLog::refill_buffer(int ix) {
    auto& str = streams_.at(ix);
    auto  buf = &read_queue_.at(ix);
    std::tie(buf->status, buf->frame) = str->read_next_frame();
    buf->pos = 0;
}

std::tuple<aku_Status, u32> ShardedInputLog::read_next(size_t  buffer_size,
                                                       u64*    idout,
                                                       u64*    tsout,
                                                       double* xsout)
{
    buffer_size = std::min(static_cast<size_t>(std::numeric_limits<u32>::max()), buffer_size);
    if (!read_started_) {
        init_read_buffers();
    }
    if (buffer_ix_ < 0) {
        // Chose buffer with smallest id. The value is initialized with negative value
        // at start.
        buffer_ix_ = choose_next();
        if (buffer_ix_ < 0) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
    }
    size_t outsize = 0;
    aku_Status outstatus = AKU_SUCCESS;
    while(buffer_ix_ >= 0 && buffer_size > 0) {
        // return values from the current buffer
        Buffer& buffer = read_queue_.at(buffer_ix_);
        if (buffer.pos < buffer.frame->data_points.size) {
            u32 toread = std::min(buffer.frame->data_points.size - buffer.pos,
                                  static_cast<u32>(buffer_size));
            const aku_ParamId*   ids = buffer.frame->data_points.ids + buffer.pos;
            const aku_Timestamp* tss = buffer.frame->data_points.tss + buffer.pos;
            const double*        xss = buffer.frame->data_points.xss + buffer.pos;
            std::copy(ids, ids + toread, idout);
            std::copy(tss, tss + toread, tsout);
            std::copy(xss, xss + toread, xsout);
            buffer_size -= toread;  // Invariant: buffer_size will never overflow, it's always less than 'toread'
            idout       += toread;
            tsout       += toread;
            xsout       += toread;
            outsize     += toread;
            buffer.pos  += toread;
        } else {
            refill_buffer(buffer_ix_);
            buffer_ix_ = choose_next();
            if (buffer_ix_ < 0) {
                // No more data
                outstatus = AKU_ENO_DATA;
                break;
            }
        }
    }
    return std::make_tuple(outstatus, outsize);
}

std::tuple<aku_Status, u32> ShardedInputLog::read_next(size_t buffer_size, InputLogRow* rows) {
    buffer_size = std::min(static_cast<size_t>(std::numeric_limits<u32>::max()), buffer_size);
    if (!read_started_) {
        init_read_buffers();
    }
    if (buffer_ix_ < 0) {
        // Chose buffer with smallest id. The value is initialized with negative value
        // at start.
        buffer_ix_ = choose_next();
        if (buffer_ix_ < 0) {
            return std::make_tuple(AKU_ENO_DATA, 0);
        }
    }
    size_t outsize = 0;
    aku_Status outstatus = AKU_SUCCESS;
    while(buffer_ix_ >= 0 && buffer_size > 0) {
        // return values from the current buffer
        Buffer& buffer = read_queue_.at(buffer_ix_);
        if (buffer.pos < buffer.frame->header.size) {
            switch (buffer.frame->header.frame_type) {
            case LZ4Volume::FrameType::DATA_ENTRY: {
                u32 toread = std::min(buffer.frame->data_points.size - buffer.pos,
                                      static_cast<u32>(buffer_size));
                const aku_ParamId*   ids = buffer.frame->data_points.ids + buffer.pos;
                const aku_Timestamp* tss = buffer.frame->data_points.tss + buffer.pos;
                const double*        xss = buffer.frame->data_points.xss + buffer.pos;
                for (u32 ix = 0; ix < toread; ix++) {
                    rows[ix].id = ids[ix];
                    InputLogDataPoint payload = {
                        tss[ix],
                        xss[ix],
                    };
                    rows[ix].payload = payload;
                }
                buffer_size -= toread;  // Invariant: buffer_size will never overflow, it's always less than 'toread'
                rows        += toread;
                buffer.pos  += toread;
                outsize     += toread;
            } break;
            case LZ4Volume::FrameType::SNAME_ENTRY: {
                u32 toread = std::min(buffer.frame->data_points.size - buffer.pos,
                                      static_cast<u32>(buffer_size));
                auto frame = reinterpret_cast<const MutableEntry*>(&buffer.frame->payload);
                for (u32 ix = 0; ix < toread; ix++) {
                    std::string sname ;
                    std::tie(rows[ix].id, sname) = frame->read_string(buffer.pos + ix);
                    InputLogSeriesName payload = {
                        sname
                    };
                    rows[ix].payload = payload;
                }
                buffer_size -= toread;
                rows        += toread;
                buffer.pos  += toread;
                outsize     += toread;
            } break;
            case LZ4Volume::FrameType::RECOVERY_ENTRY: {
                u32 toread = std::min(buffer.frame->data_points.size - buffer.pos,
                                      static_cast<u32>(buffer_size));
                auto frame = reinterpret_cast<const MutableEntry*>(&buffer.frame->payload);
                for (u32 ix = 0; ix < toread; ix++) {
                    std::vector<u64> rescue_points;
                    std::tie(rows[ix].id, rescue_points) = frame->read_array(buffer.pos + ix);
                    InputLogRecoveryInfo payload = {
                        rescue_points
                    };
                    rows[ix].payload = payload;
                }
                buffer_size -= toread;
                rows        += toread;
                buffer.pos  += toread;
                outsize     += toread;
            } break;
            case LZ4Volume::FrameType::EMPTY:
                return std::make_tuple(AKU_EBAD_DATA, 0);
            }
        } else {
            refill_buffer(buffer_ix_);
            buffer_ix_ = choose_next();
            if (buffer_ix_ < 0) {
                // No more data
                outstatus = AKU_ENO_DATA;
                break;
            }
        }
    }
    return std::make_tuple(outstatus, outsize);
}

void ShardedInputLog::reopen() {
    if (!read_only_) {
        AKU_PANIC("Can't reopen write-only input log");
    }
    streams_.clear();
    for (int i = 0; i < concurrency_; i++) {
        std::unique_ptr<InputLog> log;
        log.reset(new InputLog(rootdir_.c_str(), i));
        streams_.push_back(std::move(log));
    }
}

void ShardedInputLog::delete_files() {
    for (auto& it: streams_) {
        it->delete_files();
    }
}

std::tuple<aku_Status, int> ShardedInputLog::find_logs(const char* rootdir) {
    if (!boost::filesystem::exists(rootdir)) {
        return std::make_tuple(AKU_ENOT_FOUND, -1);
    }
    if (!boost::filesystem::is_directory(rootdir)) {
        return std::make_tuple(AKU_ENOT_FOUND, -1);
    }
    i32 max_stream_id = -1;
    for (auto it = boost::filesystem::directory_iterator(rootdir);
         it != boost::filesystem::directory_iterator(); it++) {
        boost::filesystem::path path = *it;
        bool is_volume;
        u32 volume_id;
        u32 stream_id;
        auto fn = path.filename().string();
        std::tie(is_volume, volume_id, stream_id) = parse_filename(fn);
        if (is_volume) {
            max_stream_id = std::max(static_cast<i32>(stream_id), max_stream_id);
        }
    }
    return std::make_tuple(AKU_SUCCESS, static_cast<int>(max_stream_id + 1));
}

}  // namespace
