#include <iostream>
#include <string>
#include <set>

#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp>

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
    typedef std::tuple<RecordType, std::string, std::string, double, std::string> RowT;

    virtual ~Cursor() = default;

    //! Check completion
    virtual bool done() = 0;
    //! Get next row
    virtual bool get_next_row(RowT& row) = 0;
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
    virtual void add(std::string ts, std::string id, double value) = 0;
    //! Write blob value
    virtual void add(std::string ts, std::string id, std::string const& blob) = 0;
    //! Query database
    virtual std::unique_ptr<Cursor> query(std::string begin,
                                          std::string end,
                                          std::vector<std::string> ids) = 0;
};


struct LocalCursor : Cursor {
    aku_Cursor* cursor_;
    aku_Sample  sample_;

    LocalCursor(aku_Cursor *cursor)
        : cursor_(cursor)
    {
        throw_if_error();
    }

    void throw_if_error() {
        aku_Status status = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor_, &status)) {
            throw std::runtime_error(aku_error_message(status));
        }
    }

    virtual ~LocalCursor() {
        aku_cursor_close(cursor_);
    }

    bool advance() {
        auto n_results = aku_cursor_read(cursor_, &sample_, 1);
        throw_if_error();
        // Return true if cache is not empty
        return n_results;
    }

    virtual bool done() {
        return aku_cursor_is_done(cursor_);
    }

    virtual bool get_next_row(RowT& result) {
        if (advance()) {
            // TODO: convert timestamp and paramid to strings
            if (sample_.payload.type == aku_PData::FLOAT) {
                result = std::make_tuple(
                            DOUBLE,
                            sample_.timestamp,
                            sample_.paramid,
                            sample_.payload.value.float64,
                            std::string());
            } else {
                auto begin = (const char*)sample_.payload.value.blob.begin;
                auto end = begin + sample_.payload.value.blob.size;
                std::string payload(begin, end);
                result = std::make_tuple(
                            BLOB,
                            sample_.timestamp,
                            sample_.paramid,
                            NAN,
                            payload);
            }
            return true;
        }
        return false;
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
        if (db_ == nullptr) {
            throw std::logic_error("Database allready closed");
        }
        aku_close_database(db_);
        db_ = nullptr;
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

    virtual void add(std::string ts, std::string id, double value) {
        aku_Status status = AKU_EBUSY;
        while(status == AKU_EBUSY) {
            aku_Sample sample;
            if (aku_parse_timestamp(ts.c_str(), &sample) != AKU_SUCCESS) {
                throw std::runtime_error("invalid timestamp");
            }
            if (aku_series_name_to_id(db_, id.data(), id.data() + id.size(), &sample) != AKU_SUCCESS) {
                throw std::runtime_error("invalid series name");
            }
            sample.payload.type = aku_PData::FLOAT;
            sample.payload.value.float64 = value;
            status = aku_write(db_, &sample);
        }
        throw_on_error(status);
    }

    virtual void add(std::string ts, std::string id, std::string const& blob) {
        aku_Status status = AKU_EBUSY;
        while(status == AKU_EBUSY) {
            aku_Sample sample;
            if (aku_parse_timestamp(ts.c_str(), &sample) != AKU_SUCCESS) {
                throw std::runtime_error("invalid timestamp");
            }
            if (aku_series_name_to_id(db_, id.data(), id.data() + id.size(), &sample) != AKU_SUCCESS) {
                throw std::runtime_error("invalid series name");
            }
            sample.payload.type = aku_PData::BLOB;
            sample.payload.value.blob.begin = blob.data();
            sample.payload.value.blob.size = blob.size();
            status = aku_write(db_, &sample);
        }
        throw_on_error(status);
    }

    virtual std::unique_ptr<Cursor> query(std::string begin,
                                          std::string end,
                                          std::vector<std::string> ids)
    {
        /* Query format:
         * {
         *      "sample": "all", // { "step": "5sec" } or { "random": 1000 }
         *      "metric": "cpu",
         *      // or
         *      "metric": ["cpu", "mem"],
         *      "range": {
         *          "from": "20150101T000000", // "#123456789",
         *          "to"  : "20150102T000000"
         *      },
         *      "where": [
         *          { "in" : { "key3": [1, 2, 3, "foo"]},
         *          { "not_in" : { "key4": [3, 4, 5]}
         *      ],
         *      "group_by": {
         *          "tag" : [ "host", "region" ],
         *          "metric" : [ "cpu", "memory" ]
         *      }
         * }
         */
        std::stringstream query;
        query <<
                 R"({
                    "sample": "all",
               )";
        //auto ptr = std::unique_ptr<LocalCursor>(new LocalCursor(cur));
        //return std::move(ptr);
        throw "not implemented";
    }
};


struct DataPoint {
    std::string   timestamp;
    std::string   id;
    bool          is_blob;
    double        float_value;
    std::string   blob_value;
};


std::vector<DataPoint> TEST_DATA = {
    { "20150101T000000", "cpu key=0", false, 0.0,          "" },
    { "20150101T000001", "cpu key=1",  true, NAN, "blob at 1" },
    { "20150101T000002", "cpu key=2", false, 2.2,          "" },
    { "20150101T000003", "cpu key=3",  true, NAN, "blob at 3" },
    { "20150101T000004", "cpu key=4", false, 4.4,          "" },
    { "20150101T000005", "cpu key=5",  true, NAN, "blob at 5" },
    { "20150101T000006", "cpu key=0", false, 6.6,          "" },
    { "20150101T000007", "cpu key=1",  true, NAN, "blob at 7" },
    { "20150101T000008", "cpu key=2", false, 8.8,          "" },
    { "20150101T000009", "cpu key=3",  true, NAN, "blob at 9" },
    { "20150101T000010", "cpu key=4", false, 1.0,          "" },
    { "20150101T000011", "cpu key=5",  true, NAN, "blob at 11"},
    { "20150101T000012", "cpu key=0", false, 1.2,          "" },
    { "20150101T000013", "cpu key=1",  true, NAN, "blob at 13"},
    { "20150101T000014", "cpu key=2", false, 1.4,          "" },
    { "20150101T000015", "cpu key=3",  true, NAN, "blob at 15"},
    { "20150101T000016", "cpu key=4", false, 1.6,          "" },
    { "20150101T000017", "cpu key=5",  true, NAN, "blob at 17"},
    { "20150101T000018", "cpu key=0", false, 1.8,          "" },
    { "20150101T000019", "cpu key=1",  true, NAN, "blob at 19"},
};

void add_element(Storage *storage, DataPoint& td) {
    if (td.is_blob) {
        storage->add(td.timestamp, td.id, td.blob_value);
        std::cout << "Add " << td.timestamp << ", " << td.id << ", " << td.blob_value << std::endl;
    } else {
        storage->add(td.timestamp, td.id, td.float_value);
        std::cout << "Add " << td.timestamp << ", " << td.id << ", " << td.float_value << std::endl;
    }
}

void fill_data(Storage *storage) {
    for(int i = 0; i < (int)TEST_DATA.size(); i++) {
        auto td = TEST_DATA[i];
        add_element(storage, td);
    }
}

struct Query {
    std::string            begin;
    std::string              end;
    std::vector<std::string> ids;
};

void query_data(Storage *storage, Query query, std::vector<DataPoint> expected) {
    std::unique_ptr<Cursor> cursor = storage->query(query.begin, query.end, query.ids);
    int ix = 0;
    while(!cursor->done()) {
        Cursor::RowT row;
        if (!cursor->get_next_row(row)) {
            continue;
        }
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
            std::cout << "Read " << std::get<1>(row) << ", " << std::get<2>(row) << ", " << std::get<4>(row) << std::endl;
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
            std::cout << "Read " << std::get<1>(row) << ", " << std::get<2>(row) << ", " << std::get<3>(row) << std::endl;
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

/** Query subset of the elements.
  * @param storage should point to opened storage instance
  * @param invert should be set to true to query data in backward direction
  * @param expect_empty should be set to true if expected empty result
  * @param begin beginning of the time-range (smallest timestamp)
  * @param end end of the time-range (largest timestamp)
  * @param ids should contain list of ids that we interested in
  */
void query_subset(Storage* storage, aku_Timestamp begin, aku_Timestamp end, bool invert, bool expect_empty, std::vector<aku_ParamId> ids) {
    std::cout << "===============" << std::endl;
    std::cout << "   Query subset" << std::endl;
    std::cout << "          begin = " << begin << std::endl;
    std::cout << "            end = " << end << std::endl;
    std::cout << "         invert = " << invert << std::endl;
    std::cout << "   expect_empty = " << expect_empty << std::endl;
    std::cout << "            ids = ";
    bool firstid = true;
    for(auto x: ids) {
        if (!firstid) {
            std::cout << ", ";
        }
        firstid = false;
        std::cout << x;
    }
    std::cout << std::endl;
    std::cout << "===============" << std::endl;
    assert(begin < end);
    std::set<aku_ParamId>  idsmap(ids.begin(), ids.end());
    std::vector<DataPoint> expected;
    for (int i = 0; i < (int)TEST_DATA.size(); i++) {
        auto point = TEST_DATA[i];
        if (idsmap.count(point.id) != 0 && point.timestamp >= begin && point.timestamp <= end) {
            expected.push_back(point);
        }
    }
    if (invert) {
        auto tmp = begin;
        begin = end; end = tmp;
        std::reverse(expected.begin(), expected.end());
    }
    if (expect_empty) {
        expected.clear();
    }
    Query query = { begin, end, ids };
    query_data(storage, query, expected);
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
    int retcode = 0;
    storage.create_new();
    try {
        storage.open();
        fill_data(&storage);

        {
            // In this stage all data should be cached inside the the sequencer so only
            // backward query should work fine.

            // Read in forward direction, result-set should be empty because all data is cached
            query_subset(&storage, 0ul, 20ul, false, true, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Read in backward direction, result-set shouldn't be empty
            // because cache accessed in backward direction
            query_subset(&storage, 0ul, 20ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Try to read only half of the data-points in forward direction (should be empty)
            query_subset(&storage, 5ul, 15ul, false, true, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Try to read only half of the data-points in backward direction
            query_subset(&storage, 5ul, 15ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Try to read only numeric value
            query_subset(&storage, 0ul, 20ul, true, false, {0ul, 2ul, 4ul});
            // Try to read only BLOB values
            query_subset(&storage, 0ul, 20ul, true, false, {1ul, 3ul, 5ul});

            storage.close();
        }

        {
            // Database is reopened. At this stage everything should be readable in both directions.
            storage.open();

            query_subset(&storage, 0ul, 20ul, false, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            query_subset(&storage, 0ul, 20ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});

            // Filter by timestamp
            query_subset(&storage, 5ul, 15ul, false, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            query_subset(&storage, 5ul, 15ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});

            // Filter out BLOBs
            query_subset(&storage, 0ul, 20ul, true, false, {0ul, 2ul, 4ul});
            query_subset(&storage, 0ul, 20ul, false, false, {0ul, 2ul, 4ul});
            // Filter out numeric values
            query_subset(&storage, 0ul, 20ul, true, false, {1ul, 3ul, 5ul});
            query_subset(&storage, 0ul, 20ul, false, false, {1ul, 3ul, 5ul});

            storage.close();
        }

        {
            storage.open();
            // Add some data
            DataPoint newpoints[] = {
                {20ul, 2ul, false, 2.0, "" },
                {21ul, 3ul, true,  NAN, "blob at 21"},
                {22ul, 4ul, false, 2.2, "" },
                {23ul, 5ul, true,  NAN, "blob at 23"},
            };
            for (int i = 0; i < 4; i++) {
                TEST_DATA.push_back(newpoints[i]);
                add_element(&storage, newpoints[i]);
            }
            // Read in forward direction, result-set should be empty because all new data is cached
            query_subset(&storage, 20ul, 25ul, false, true, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Read in backward direction, result-set shouldn't be empty
            // because cache accessed in backward direction
            query_subset(&storage, 20ul, 25ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            // Query all in rev. direction, everything should be in place
            query_subset(&storage, 0ul, 20ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});

            // Filter out BLOBs
            query_subset(&storage, 0ul, 24ul, true, false, {0ul, 2ul, 4ul});
            // Filter out numeric values
            query_subset(&storage, 0ul, 24ul, true, false, {1ul, 3ul, 5ul});

            storage.close();
        }

        {
            storage.open();

            // All new data should be readable
            query_subset(&storage, 0ul, 24ul, false, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            query_subset(&storage, 0ul, 24ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});

            // Filter by timestamp
            query_subset(&storage, 5ul, 15ul, false, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});
            query_subset(&storage, 5ul, 15ul, true, false, {0ul, 1ul, 2ul, 3ul, 4ul, 5ul});

            // Filter out BLOBs
            query_subset(&storage, 0ul, 24ul, true, false, {0ul, 2ul, 4ul});
            query_subset(&storage, 0ul, 24ul, false, false, {0ul, 2ul, 4ul});

            // Filter out numeric values
            query_subset(&storage, 0ul, 24ul, true, false, {1ul, 3ul, 5ul});
            query_subset(&storage, 0ul, 24ul, false, false, {1ul, 3ul, 5ul});

            storage.close();
        }

    } catch (...) {
        std::cout << boost::current_exception_diagnostic_information() << std::endl;
        retcode = -1;
    }
    storage.delete_all();
    return retcode;
}
