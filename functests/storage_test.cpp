#include <iostream>
#include <string>

#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>

#include "akumuli.h"

const std::string DEFAULT_DIR = "/tmp";

void print_help() {
    std::cout << "Storage engine functional tests" << std::endl;
    std::cout << "-------------------------------" << std::endl;
    std::cout << "param: working directory (default: /tmp)" << std::endl;
    std::cout << "example: ./storage_test ./home/work" << std::endl;
}

bool check_path_exists(std::string path) {
    struct stat s = {0};
    if (!stat(path.c_str(), &s)) {
        return S_ISDIR(s.st_mode);
    }
    return false;
}

/** Row iterator interface
  */
struct Cursor {
    enum RecordType {
        DOUBLE,
        BLOB,
    };
    typedef std::tuple<RecordType, aku_Timestamp, aku_ParamId, double, std::string> RowT;

    virtual ~Cursor() = default;

    //! Check completion
    virtual bool done() = 0;
    //! Get next row
    virtual RowT get_next_row() = 0;
};

/** Storage wrapper class. Allows to test seamlessly local libakumuli instance and akumulid daemon
  * deployed on AWS instance.
  */
struct Storage {
    virtual ~Storage() = default;
    //! Create new storage (create file on disk without opening database)
    virtual void create_new() = 0;
    //! Open storage
    virtual void open() = 0;
    //! Close storage
    virtual void close() = 0;
    //! Delete files on disk (database should be closed)
    virtual void delete_all() = 0;
    //! Write numeric value
    virtual void add(aku_Timestamp ts, aku_ParamId id, double value) = 0;
    //! Write blob value
    virtual void add(aku_Timestamp ts, aku_ParamId id, std::string const& blob) = 0;
    //! Query database
    virtual std::unique_ptr<Cursor> query(aku_Timestamp begin,
                                          aku_Timestamp end,
                                          std::vector<aku_ParamId> ids) = 0;
};


struct LocalCursor : Cursor {
    aku_Cursor* cursor_;
    std::vector<aku_Timestamp> timestamps_;
    std::vector<aku_ParamId>   paramids_;
    std::vector<aku_PData>     payload_;
    std::vector<uint32_t>      lengths_;
    int rows_in_cache_;
    int ix_row_;
    const int CACHE_SIZE;

    LocalCursor(aku_Cursor *cursor)
        : cursor_(cursor)
        , rows_in_cache_(0)
        , ix_row_(0)
        , CACHE_SIZE(100)
    {
        throw_if_error();
        timestamps_.reserve(CACHE_SIZE);
        paramids_.reserve(CACHE_SIZE);
        payload_.reserve(CACHE_SIZE);
        lengths_.reserve(CACHE_SIZE);
    }

    void throw_if_error() {
        aku_Status status = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor_, &status)) {
            throw std::runtime_error(aku_error_message(status));
        }
    }

    virtual ~LocalCursor() {
        aku_close_cursor(cursor_);
    }

    virtual bool done() {
        throw_if_error();
        return aku_cursor_is_done(cursor_);
    }

    virtual RowT get_next_row() {
        if (rows_in_cache_ == ix_row_) {
            rows_in_cache_ = aku_cursor_read_columns(cursor_,
                                                     timestamps_.data(),
                                                     paramids_.data(),
                                                     payload_.data(),
                                                     lengths_.data(),
                                                     CACHE_SIZE);
            ix_row_ = 0;
            throw_if_error();
        }
        RowT result;
        auto len = lengths_.at(ix_row_);
        if (len == 0) {
            result = std::make_tuple(
                        DOUBLE,
                        timestamps_.at(ix_row_),
                        paramids_.at(ix_row_),
                        payload_.at(ix_row_).float64,
                        std::string());
        } else {
            aku_PData data = payload_.at(ix_row_);
            auto begin = static_cast<const char*>(data.ptr);
            auto end = begin + len;
            std::string payload(begin, end);
            result = std::make_tuple(
                        BLOB,
                        timestamps_.at(ix_row_),
                        paramids_.at(ix_row_),
                        NAN,
                        payload);
        }
        ix_row_++;
        return result;
    }
};


struct LocalStorage : Storage {
    const std::string work_dir_;
    const uint32_t compression_threshold_;
    const uint64_t sliding_window_size_;
    const int32_t n_volumes_;
    const uint32_t durability_;
    bool enable_huge_tlb_;
    const char* DBNAME_;
    aku_Database *db_;

    LocalStorage(
            std::string work_dir,
            // Creation parameters, used only to create database
            uint32_t compression_threshold,
            uint64_t sliding_window_size,
            int32_t n_volumes,
            // Open parameters, used unly to open database
            uint32_t durability = 1u,
            bool huge_tlb = false
            )
        : work_dir_(work_dir)
        , compression_threshold_(compression_threshold)
        , sliding_window_size_(sliding_window_size)
        , n_volumes_(n_volumes)
        , durability_(durability)
        , enable_huge_tlb_(huge_tlb)
        , DBNAME_("test")
        , db_(nullptr)
    {
    }

    void throw_on_error(aku_Status status) const {
        if (status != AKU_SUCCESS) {
            throw std::runtime_error(aku_error_message(status));
        }
    }

    std::string get_db_file_path() const {
        std::string path = work_dir_ + "/" + DBNAME_ + ".akumuli";
        return path;
    }

    // Storage interface
    virtual void close() {
        aku_close_database(db_);
    }

    virtual void create_new()
    {
        apr_status_t result = aku_create_database(DBNAME_, work_dir_.c_str(), work_dir_.c_str(), n_volumes_,
                                                  compression_threshold_, sliding_window_size_, 0, nullptr);
        throw_on_error(result);
    }

    virtual void open()
    {
        if (db_ != nullptr) {
            throw std::logic_error("Database allready opened");
        }
        aku_FineTuneParams params;
        params.durability = durability_;
        params.enable_huge_tlb = enable_huge_tlb_ ? 1 : 0;
        params.logger = &aku_console_logger;
        std::string path = get_db_file_path();
        db_ = aku_open_database(path.c_str(), params);
        auto status = aku_open_status(db_);
        throw_on_error(status);
    }

    virtual void delete_all()
    {
        std::string path = get_db_file_path();
        auto status = aku_remove_database(path.c_str(), &aku_console_logger);
        throw_on_error(status);
    }

    virtual void add(aku_Timestamp ts, aku_ParamId id, double value) {
        aku_Status status = AKU_EBUSY;
        while(status == AKU_EBUSY) {
            status = aku_write_double_raw(db_, id, ts, value);
        }
        throw_on_error(status);
    }

    virtual void add(aku_Timestamp ts, aku_ParamId id, std::string const& blob) {
        aku_Status status = AKU_EBUSY;
        aku_MemRange range = { (void*)blob.data(), (uint32_t)blob.size() };
        while(status == AKU_EBUSY) {
            status = aku_write_blob(db_, id, ts, range);
        }
        throw_on_error(status);
    }

    virtual std::unique_ptr<Cursor> query(aku_Timestamp begin,
                                          aku_Timestamp end,
                                          std::vector<aku_ParamId> ids)
    {
        aku_SelectQuery *query = aku_make_select_query(begin, end, (uint32_t)ids.size(), ids.data());
        auto cur = aku_select(db_, query);
        aku_destroy(query);
        auto ptr = std::unique_ptr<LocalCursor>(new LocalCursor(cur));
        return std::move(ptr);
    }
};


struct DataPoint {
    aku_Timestamp timestamp;
    aku_ParamId   id;
    bool          is_blob;
    double        float_value;
    std::string   blob_value;
};


const DataPoint TEST_DATA[] = {
    { 0ul, 0ul, false, 0.0, "" },
    { 1ul, 1ul, true,  NAN, "blob at 1" },
    { 2ul, 2ul, false, 2.2, "" },
    { 3ul, 3ul, true,  NAN, "blob at 3" },
    { 4ul, 4ul, false, 4.4, "" },
    { 5ul, 0ul, true,  NAN, "blob at 5" },
    { 6ul, 1ul, false, 6.6, "" },
    { 7ul, 2ul, true,  NAN, "blob at 7" },
    { 8ul, 3ul, false, 8.8, "" },
    { 9ul, 4ul, true,  NAN, "blob at 9" },
    {10ul, 0ul, false, 1.0, "" },
    {11ul, 1ul, true,  NAN, "blob at 11"},
    {12ul, 2ul, false, 1.2, "" },
    {13ul, 3ul, true,  NAN, "blob at 13"},
    {14ul, 4ul, false, 1.4, "" },
    {15ul, 0ul, true,  NAN, "blob at 13"},
    {16ul, 1ul, false, 1.6, "" },
    {17ul, 2ul, true,  NAN, "blob at 17"},
    {18ul, 3ul, false, 1.8, "" },
    {19ul, 4ul, true,  NAN, "blob at 19"},
};

const int TEST_DATA_LEN = sizeof(TEST_DATA)/sizeof(DataPoint);


void fill_data(Storage *storage) {
    for(int i = 0; i < TEST_DATA_LEN; i++) {
        auto td = TEST_DATA[i];
        if (td.is_blob) {
            storage->add(td.timestamp, td.id, td.blob_value);
            std::cout << "Add " << td.timestamp << ", " << td.id << ", " << td.blob_value << std::endl;
        } else {
            storage->add(td.timestamp, td.id, td.float_value);
            std::cout << "Add " << td.timestamp << ", " << td.id << ", " << td.float_value << std::endl;
        }
    }
}

struct Query {
    aku_Timestamp          begin;
    aku_Timestamp            end;
    std::vector<aku_ParamId> ids;
};

void query_data(Storage *storage, Query query, std::vector<DataPoint> expected) {
    std::unique_ptr<Cursor> cursor = storage->query(query.begin, query.end, query.ids);
    int ix = 0;
    while(!cursor->done()) {
        auto row = cursor->get_next_row();
        DataPoint exp = expected.at(ix++);
        if (std::get<1>(row) != exp.timestamp) {
            std::cout << "Error at " << ix << std::endl;
            std::cout << "bad timestamp, get " << std::get<1>(row)
                      << ", expected " << exp.timestamp << std::endl;
            throw std::runtime_error("Bad result");
        }
        if (std::get<2>(row) != exp.id) {
            std::cout << "Error at " << ix << std::endl;
            std::cout << "bad id, get " << std::get<2>(row)
                      << ", expected " << exp.id << std::endl;
            throw std::runtime_error("Bad result");
        }
        if (std::get<0>(row) == Cursor::BLOB) {
            if (!exp.is_blob) {
                std::cout << "Error at " << ix << std::endl;
                std::cout << "blob expected"   << std::endl;
                throw std::runtime_error("Bad result");
            }
            if (std::get<4>(row) != exp.blob_value) {
                std::cout << "Error at " << ix << std::endl;
                std::cout << "bad BLOB, get " << std::get<4>(row)
                          << ", expected " << exp.blob_value << std::endl;
                throw std::runtime_error("Bad result");
            }
        } else {
            if (exp.is_blob) {
                std::cout << "Error at " << ix << std::endl;
                std::cout << "float expected"   << std::endl;
                throw std::runtime_error("Bad result");
            }
            if (std::get<3>(row) != exp.float_value) {
                std::cout << "Error at " << ix << std::endl;
                std::cout << "bad float, get " << std::get<3>(row)
                          << ", expected " << exp.float_value << std::endl;
                throw std::runtime_error("Bad result");
            }
        }
    }
    if (ix != (int)expected.size()) {
        std::cout << "Not enough data read" << std::endl;
        std::cout << "expected " << expected.size() << " values but only "
                  << ix << " was read from DB" << std::endl;
        throw std::runtime_error("Not enough results");
    }
}


int main(int argc, const char** argv) {
    std::string dir;
    if (argc == 1) {
        dir = DEFAULT_DIR;
    } else if (argc == 2) {
        dir = argv[1];
        if (dir == "--help") {
            print_help();
            return 0;
        }
        if (!check_path_exists(dir)) {
            std::cout << "Invalid path" << std::endl;
            return 2;
        }
    }
    if (boost::ends_with(dir, "/")) {
        dir.pop_back();
    }
    std::cout << "Working directory: " << dir << std::endl;
    aku_initialize(nullptr);

    uint32_t compression_threshold = 5;
    uint64_t windowsize = 10;
    LocalStorage storage(dir, compression_threshold, windowsize, 2);

    // Try to delete old data if any
    try {
        storage.delete_all();
    } catch(std::runtime_error const&) {
        // No old data
    }

    storage.create_new();

    storage.open();

    fill_data(&storage);

    storage.close();

    storage.delete_all();
    
    return 0;
}
