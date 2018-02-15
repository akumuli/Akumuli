#include "input_log.h"
#include "log_iface.h"

#include <regex>
#include <boost/lexical_cast.hpp>
#include <chrono>

namespace Akumuli {

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
    return std::move(pool);
}

static AprFilePtr _open_file(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_WRITE|APR_BINARY|APR_CREATE, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
}

static AprFilePtr _open_file_ro(const char* file_name, apr_pool_t* pool) {
    apr_file_t* pfile = nullptr;
    apr_status_t status = apr_file_open(&pfile, file_name, APR_READ|APR_BINARY, APR_OS_DEFAULT, pool);
    panic_on_error(status, "Can't open file");
    AprFilePtr file(pfile, &_close_apr_file);
    return std::move(file);
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

static u32 now() {
    auto now = std::chrono::steady_clock::now();
    auto now_sec = std::chrono::time_point_cast<std::chrono::seconds>(now);
    return now_sec.time_since_epoch().count();
}

void LZ4Volume::clear(int i) {
    memset(&frames_[i], 0, BLOCK_SIZE);
}

aku_Status LZ4Volume::write(int i) {
    assert(!is_read_only_);
    Frame& frame = frames_[i];
    frame.part.end_timestamp = now();
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
    if (status == AKU_SUCCESS) {
        file_size_ += size;
    }
    return status;
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

LZ4Volume::LZ4Volume(const char* file_name, size_t volume_size)
    : path_(file_name)
    , pos_(0)
    , pool_(_make_apr_pool())
    , file_(_open_file(file_name, pool_.get()))
    , file_size_(0)
    , max_file_size_(volume_size)
    , is_read_only_(false)
    , bytes_to_read_(0)
    , elements_to_read_(0)
{
    clear(0);
    clear(1);
    LZ4_resetStream(&stream_);
}

LZ4Volume::LZ4Volume(const char* file_name)
    : path_(file_name)
    , pos_(1)
    , pool_(_make_apr_pool())
    , file_(_open_file_ro(file_name, pool_.get()))
    , file_size_(_get_file_size(file_.get()))
    , max_file_size_(0)
    , is_read_only_(true)
    , bytes_to_read_(file_size_)
    , elements_to_read_(0)
{
    clear(0);
    clear(1);
    LZ4_setStreamDecode(&decode_stream_, NULL, 0);
}

LZ4Volume::~LZ4Volume() {
    if (file_) {
        close();
    }
}

size_t LZ4Volume::file_size() const {
    return file_size_;
}

void LZ4Volume::close() {
    if(!is_read_only_) {
        // Write unfinished frame if it contains any data.
        if (frames_[pos_].part.size != 0) {
            write(pos_);
        }
    }
    file_.reset();
}

aku_Status LZ4Volume::append(u64 id, u64 timestamp, double value) {
    bitmap_.add(id);
    Frame& frame = frames_[pos_];
    if (frame.part.size == 0) {
        frame.part.begin_timestamp = now();
    }
    frame.part.ids[frame.part.size] = id;
    frame.part.tss[frame.part.size] = timestamp;
    frame.part.xss[frame.part.size] = value;
    frame.part.size++;
    if (frame.part.size == NUM_TUPLES) {
        auto status = write(pos_);
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
        elements_to_read_ = frames_[pos_].part.size;
    }
    Frame& frame = frames_[pos_];
    size_t nvalues = std::min(buffer_size, static_cast<size_t>(elements_to_read_));
    size_t frmsize = frame.part.size;
    for (size_t i = 0; i < nvalues; i++) {
        size_t ix = frmsize - elements_to_read_;
        id[i] = frame.part.ids[ix];
        ts[i] = frame.part.tss[ix];
        xs[i] = frame.part.xss[ix];
        elements_to_read_--;
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
    return bitmap_;
}


//          //
// InputLog //
//          //

static std::tuple<bool, u32, u32> parse_filename(const std::string& name) {
    static const char* exp = "inputlog(\\d+)_(\\d+)\\.ils";
    static const std::regex re(exp);
    std::smatch smatch;
    if (std::regex_search(name, smatch, re) && smatch.size() > 2) {
        auto volume_id = boost::lexical_cast<u32>(smatch[1].str());
        auto stream_id = boost::lexical_cast<u32>(smatch[2].str());
        return std::make_tuple(true, volume_id, stream_id);
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
            volumes.push_back(std::make_pair(volume_id, fn));
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
    std::unique_ptr<LZ4Volume> volume(new LZ4Volume(path.c_str(), volume_size_));
    volumes_.push_front(std::move(volume));
    volume_counter_++;
}

void InputLog::remove_last_volume() {
    auto volume = std::move(volumes_.back());
    volumes_.pop_back();
    volume->delete_file();
    Logger::msg(AKU_LOG_INFO, std::string("Remove volume ") + volume->get_path());
}

InputLog::InputLog(const char* rootdir, size_t nvol, size_t svol, u32 stream_id)
    : root_dir_(rootdir)
    , volume_counter_(0)
    , max_volumes_(nvol)
    , volume_size_(svol)
    , stream_id_(stream_id)
{
    std::string path = get_volume_name();
    add_volume(path);
}

InputLog::InputLog(const char* rootdir, u32 stream_id)
    : root_dir_(rootdir)
    , volume_counter_(0)
    , max_volumes_(0)
    , volume_size_(0)
    , stream_id_(stream_id)
{
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

aku_Status InputLog::append(u64 id, u64 timestamp, double value, std::vector<u64>* stale_ids) {
    aku_Status result = volumes_.front()->append(id, timestamp, value);
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

std::tuple<aku_Status, u32> InputLog::read_next(size_t buffer_size, u64* id, u64* ts, double* xs) {
    while(true) {
        if (volumes_.empty()) {
            return std::make_tuple(AKU_SUCCESS, 0);
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

std::tuple<aku_Status, const LZ4Volume::Frame*> InputLog::read_next_frame() {
    while(true) {
        if (volumes_.empty()) {
            return std::make_tuple(AKU_SUCCESS, nullptr);
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
}


//                 //
// ShardedInputLog //
//                 //

ShardedInputLog::ShardedInputLog(int concurrency,
                                 const char* rootdir,
                                 size_t nvol,
                                 size_t svol)
    : concurrency_(concurrency)
{
    for (int i = 0; i < concurrency_; i++) {
        std::unique_ptr<InputLog> log;
        log.reset(new InputLog(rootdir, nvol, svol, i));
        streams_.push_back(std::move(log));
    }
}

InputLog& ShardedInputLog::get_shard(int i) {
    return *streams_.at(i);
}

void ShardedInputLog::init_read_buffers() {
    assert(!read_started_);
    read_queue_.resize(concurrency_);
    for (size_t i = 0; i < read_queue_.size(); i++) {
        auto& str = streams_.at(i);
        auto  buf = &read_queue_.at(i);
        std::tie(buf->status, buf->size) = str->read_next(NUM_TUPLES, buf->ids, buf->tss, buf->xss);
        buf->pos = 0;
    }
    read_started_ = true;
}

std::tuple<aku_Status, u32> ShardedInputLog::read_next(size_t  buffer_size,
                                                       u64*    id,
                                                       u64*    ts,
                                                       double* xs)
{
    if (!read_started_) {
        init_read_buffers();
    }
    throw "not implemented";
}

}  // namespace
