#include <iostream>
#include <string>
#include <set>
#include <cmath>

#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

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
    struct RowT {
        std::string timestamp;
        std::string seriesname;
        double value;
        // raw values
        aku_ParamId rawid;
        aku_Timestamp rawts;
    };

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
    //! Query database
    virtual std::unique_ptr<Cursor> query(std::string begin,
                                          std::string end,
                                          std::vector<std::string> ids) = 0;
    //! Query series names
    virtual std::unique_ptr<Cursor> metadata_query(std::string metric,
                                                   std::string where_clause) = 0;
};

boost::property_tree::ptree from_json(std::string json) {
    //! C-string to streambuf adapter
    struct MemStreambuf : std::streambuf {
        MemStreambuf(const char* buf) {
            char* p = const_cast<char*>(buf);
            setg(p, p, p+strlen(p));
        }
    };

    boost::property_tree::ptree ptree;
    MemStreambuf strbuf(json.c_str());
    std::istream stream(&strbuf);
    boost::property_tree::json_parser::read_json(stream, ptree);
    return ptree;
}

struct LocalCursor : Cursor {
    aku_Session* session_;
    aku_Cursor*  cursor_;
    aku_Sample   sample_;

    LocalCursor(aku_Session *s, aku_Cursor *cursor)
        : session_(s)
        , cursor_(cursor)
        , sample_()
    {
        can_proceed();
    }

    bool can_proceed() {
        aku_Status status = AKU_SUCCESS;
        if (aku_cursor_is_error(cursor_, &status)) {
            std::runtime_error err(aku_error_message(status));
            BOOST_THROW_EXCEPTION(err);
        }
        return true;
    }

    virtual ~LocalCursor() {
        aku_cursor_close(cursor_);
    }

    bool advance() {
        auto n_results = aku_cursor_read(cursor_, &sample_, sizeof(aku_Sample));
        if (can_proceed()) {
            // Return true if cache is not empty
            assert(n_results == 0 || n_results == sizeof(aku_Sample));
            return n_results;
        }
        return false;
    }

    virtual bool done() {
        if (cursor_) {
            return aku_cursor_is_done(cursor_);
        }
        return true;
    }

    virtual bool get_next_row(RowT& result) {
        if (advance()) {
            const int buffer_size = AKU_LIMITS_MAX_SNAME;
            char buffer[buffer_size];
            // Convert id
            auto len = aku_param_id_to_series(session_, sample_.paramid, buffer, buffer_size);
            if (len <= 0) {
                std::runtime_error err("no such id");
                BOOST_THROW_EXCEPTION(err);
            }
            std::string paramid(buffer, buffer + len);

            std::string timestamp;
            if (sample_.payload.type & aku_PData::TIMESTAMP_BIT) {
                // Convert timestamp
                len = aku_timestamp_to_string(sample_.timestamp, buffer, buffer_size);
                if (len <= 0) {
                    std::runtime_error err("bad timestamp");
                    BOOST_THROW_EXCEPTION(err);
                }
                timestamp = std::string(buffer, buffer + len - 1);
            }
            // Convert payload
            if (sample_.payload.type & aku_PData::FLOAT_BIT) {
                result = { timestamp, paramid, sample_.payload.float64, sample_.paramid, sample_.timestamp };
            } else {
                result = { std::string(), paramid, NAN, sample_.paramid, sample_.timestamp };
            }
            return true;
        }
        return false;
    }
};


struct LocalStorage : Storage {
    const std::string work_dir_;
    const u32 compression_threshold_;
    const u64 sliding_window_size_;
    const i32 n_volumes_;
    const u32 durability_;
    bool enable_huge_tlb_;
    bool enable_allocate_;
    const char* DBNAME_;
    aku_Database *db_;
    aku_Session  *session_;

    LocalStorage(
            std::string work_dir,
            // Creation parameters, used only to create database
            u32 compression_threshold,
            u64 sliding_window_size,
            i32 n_volumes,
            // Open parameters, used unly to open database
            u32 durability = 1u,
            bool huge_tlb = false,
            bool allocate = false
            )
        : work_dir_(work_dir)
        , compression_threshold_(compression_threshold)
        , sliding_window_size_(sliding_window_size)
        , n_volumes_(n_volumes)
        , durability_(durability)
        , enable_huge_tlb_(huge_tlb)
        , enable_allocate_(allocate)
        , DBNAME_("test")
        , db_(nullptr)
        , session_(nullptr)
    {
    }

    void throw_on_error(aku_Status status) const {
        if (status != AKU_SUCCESS) {
            std::runtime_error err(aku_error_message(status));
            BOOST_THROW_EXCEPTION(err);
        }
    }

    void throw_on_error(apr_status_t status) const {
        if (status != APR_SUCCESS) {
            char buffer[1000];
            apr_strerror(status, buffer, 1000);
            std::runtime_error err(buffer);
            BOOST_THROW_EXCEPTION(err);
        }
    }

    std::string get_db_file_path() const {
        std::string path = work_dir_ + "/" + DBNAME_ + ".akumuli";
        return path;
    }

    virtual void create_new();
    virtual void open();
    virtual void close();
    virtual void delete_all();
    virtual void add(std::string ts, std::string id, double value);
    virtual std::unique_ptr<Cursor> query(std::string begin, std::string end, std::vector<std::string> ids);
    virtual std::unique_ptr<Cursor> metadata_query(std::string metric, std::string where_clause);
};

// Storage interface
void LocalStorage::close() {
    if (session_ == nullptr || db_ == nullptr) {
        std::logic_error err("Database allready closed");
        BOOST_THROW_EXCEPTION(err);
    }
    aku_destroy_session(session_);
    aku_close_database(db_);
    db_ = nullptr;
}

void LocalStorage::create_new() {
    apr_status_t result = aku_create_database(DBNAME_, work_dir_.c_str(), work_dir_.c_str(), n_volumes_, enable_allocate_);
    throw_on_error(result);
}

void LocalStorage::open() {
    if (db_ != nullptr) {
        std::logic_error err("Database allready opened");
        BOOST_THROW_EXCEPTION(err);
    }
    aku_FineTuneParams params = {};
    params.durability = durability_;
    params.enable_huge_tlb = enable_huge_tlb_ ? 1 : 0;
    params.logger = &aku_console_logger;
    params.compression_threshold = compression_threshold_;
    params.window_size = sliding_window_size_;

    std::string path = get_db_file_path();
    db_ = aku_open_database(path.c_str(), params);
    session_ = aku_create_session(db_);
    //auto status = aku_open_status(db_);
    //throw_on_error(status);
}

void LocalStorage::delete_all() {
    std::string path = get_db_file_path();
    auto status = aku_remove_database(path.c_str(), true);
    throw_on_error(status);
}

void LocalStorage::add(std::string ts, std::string id, double value) {
    aku_Status status = AKU_EBUSY;
    while(status == AKU_EBUSY) {
        aku_Sample sample;
        if (aku_parse_timestamp(ts.c_str(), &sample) != AKU_SUCCESS) {
            std::runtime_error err("invalid timestamp");
            BOOST_THROW_EXCEPTION(err);
        }
        if (aku_series_to_param_id(session_, id.data(), id.data() + id.size(), &sample) != AKU_SUCCESS) {
            std::runtime_error err("invalid series name");
            BOOST_THROW_EXCEPTION(err);
        }
        sample.payload.type = AKU_PAYLOAD_FLOAT;
        sample.payload.float64 = value;
        status = aku_write(session_, &sample);
    }
    throw_on_error(status);
}

std::unique_ptr<Cursor> LocalStorage::query(std::string begin,
                                            std::string end,
                                            std::vector<std::string> ids)
{
    boost::property_tree::ptree query;
    // Add metric name
    query.add("select", "cpu");
    // Add time constraints
    boost::property_tree::ptree range;
    range.add("from", begin);
    range.add("to", end);
    query.add_child("range", range);
    // Where clause
    // Where clause
    boost::property_tree::ptree where;
    boost::property_tree::ptree array;
    for (auto series: ids) {
        auto val = series.substr(8, 1);
        boost::property_tree::ptree elem;
        elem.put("", val);
        array.push_back(std::make_pair("", elem));
    }
    where.add_child("key", array);
    query.add_child("where", where);
    std::stringstream stream;
    boost::property_tree::json_parser::write_json(stream, query, true);
    std::string query_text = stream.str();

    auto cursor = aku_query(session_, query_text.c_str());
    std::unique_ptr<LocalCursor> ptr(new LocalCursor(session_, cursor));
    return std::move(ptr);
}

std::unique_ptr<Cursor> LocalStorage::metadata_query(std::string metric, std::string where_clause) {
    boost::property_tree::ptree query;

    if (metric.empty()) {
        query.add("select", "meta:names");
    } else {
        query.add("select", "meta:names:" + metric);
    }

    // Where clause
    if (!where_clause.empty()) {
        boost::property_tree::ptree where = from_json(where_clause);
        query.add_child("where", where);
    }

    std::stringstream stream;
    boost::property_tree::json_parser::write_json(stream, query, true);
    std::string query_text = stream.str();

    auto cursor = aku_query(session_, query_text.c_str());
    std::unique_ptr<LocalCursor> ptr(new LocalCursor(session_, cursor));
    return std::move(ptr);
}


struct DataPoint {
    std::string   timestamp;
    std::string   id;
    double        float_value;
};


static std::vector<DataPoint> TEST_DATA = {
    { "20150101T000000.000000000", "cpu key=0", 0.0 },
    { "20150101T000001.000000000", "cpu key=1", 1.1 },
    { "20150101T000002.000000000", "cpu key=2", 2.2 },
    { "20150101T000003.000000000", "cpu key=3", 3.3 },
    { "20150101T000004.000000000", "cpu key=4", 4.4 },
    { "20150101T000005.000000000", "cpu key=5", 5.5 },
    { "20150101T000006.000000000", "cpu key=0", 6.6 },
    { "20150101T000007.000000000", "cpu key=1", 7.7 },
    { "20150101T000008.000000000", "cpu key=2", 8.8 },
    { "20150101T000009.000000000", "cpu key=3", 9.9 },
    { "20150101T000010.000000000", "cpu key=4", 1.0 },
    { "20150101T000011.000000000", "cpu key=5", 1.1 },
    { "20150101T000012.000000000", "cpu key=0", 1.2 },
    { "20150101T000013.000000000", "cpu key=1", 1.3 },
    { "20150101T000014.000000000", "cpu key=2", 1.4 },
    { "20150101T000015.000000000", "cpu key=3", 1.5 },
    { "20150101T000016.000000000", "cpu key=4", 1.6 },
    { "20150101T000017.000000000", "cpu key=5", 1.7 },
    { "20150101T000018.000000000", "cpu key=0", 1.8 },
    { "20150101T000019.000000000", "cpu key=1", 1.9 },
};

void add_element(Storage *storage, DataPoint& td) {
    storage->add(td.timestamp, td.id, td.float_value);
    std::cout << "Add " << td.timestamp << ", " << td.id << ", " << td.float_value << std::endl;
}

void fill_data(Storage *storage) {
    for(size_t i = 0; i < TEST_DATA.size(); i++) {
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
    size_t ix = 0;
    while(!cursor->done()) {
        Cursor::RowT row;
        if (!cursor->get_next_row(row)) {
            continue;
        }
        DataPoint exp = expected.at(ix++);
        if (row.timestamp != exp.timestamp) {
            std::cout << "Error at " << ix << std::endl;
            std::cout << "bad timestamp, get " << row.timestamp
                      << ", expected " << exp.timestamp << std::endl;
            std::runtime_error err("Bad result");
            BOOST_THROW_EXCEPTION(err);
        }
        if (row.seriesname != exp.id) {
            std::cout << "Error at " << ix << std::endl;
            std::cout << "bad id, get " << row.seriesname << " (" << row.rawid << ")"
                      << ", expected " << exp.id << std::endl;
            std::runtime_error err("Bad result");
            BOOST_THROW_EXCEPTION(err);
        }
        // payload
        #ifdef VERBOSE_OUTPUT
        std::cout << "Read " << row.seriesname << ", " << row.timestamp << ", " << row.value << std::endl;
        #endif
        if (row.value != exp.float_value) {
            std::cout << "Error at " << ix << std::endl;
            std::cout << "bad float, get " << row.value
                      << ", expected " << exp.float_value << std::endl;
            std::runtime_error err("Bad result");
            BOOST_THROW_EXCEPTION(err);
        }
    }
    if (ix != expected.size()) {
        std::cout << "Not enough data read" << std::endl;
        std::cout << "expected " << expected.size() << " values but only "
                  << ix << " was read from DB" << std::endl;
        std::runtime_error err("Not enough results");
        BOOST_THROW_EXCEPTION(err);
    }
}

aku_Timestamp to_timestamp(std::string ts) {
    aku_Sample s;
    if (aku_parse_timestamp(ts.c_str(), &s) != AKU_SUCCESS) {
        std::runtime_error err("bad timestamp string");
        BOOST_THROW_EXCEPTION(err);
    }
    return s.timestamp;
}

std::string to_string(aku_Timestamp ts) {
    const int bufsz = 0x100;
    char buf[bufsz];
    int len = aku_timestamp_to_string(ts, buf, bufsz);
    if (len <= 0) {
        std::runtime_error err("bad timestamp");
        BOOST_THROW_EXCEPTION(err);
    }
    return std::string(buf, buf + bufsz);
}

/** Query subset of the elements.
  * @param storage should point to opened storage instance
  * @param invert should be set to true to query data in backward direction
  * @param expect_empty should be set to true if expected empty result
  * @param begin beginning of the time-range (smallest timestamp)
  * @param end end of the time-range (largest timestamp)
  * @param ids should contain list of ids that we interested in
  */
void query_subset(Storage* storage, std::string begin, std::string end, bool invert, bool expect_empty, std::vector<std::string> ids) {
    auto begin_ts = to_timestamp(begin);
    auto end_ts = to_timestamp(end);
#ifdef VERBOSE_OUTPUT
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
#endif
    assert(to_timestamp(begin) < to_timestamp(end));
    std::set<std::string>  idsmap(ids.begin(), ids.end());
    std::vector<DataPoint> expected;
    for (size_t i = 0; i < TEST_DATA.size(); i++) {
        auto point = TEST_DATA[i];
        auto id_match = idsmap.count(point.id);
        auto point_ts = to_timestamp(point.timestamp);
        if (invert) {
            if (id_match &&
                point_ts > begin_ts &&
                point_ts <= end_ts)
            {
                expected.push_back(point);
            }
        } else {
            if (id_match &&
                point_ts >= begin_ts &&
                point_ts < end_ts)
            {
                expected.push_back(point);
            }
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

void query_metadata(Storage* storage, std::string metric, std::string where_clause, std::vector<std::string> expected) {
    std::unique_ptr<Cursor> cursor = storage->metadata_query(metric, where_clause);
    std::vector<std::string> actual;
    while(!cursor->done()) {
        Cursor::RowT row;
        if(!cursor->get_next_row(row)) {
            continue;
        }
        actual.push_back(row.seriesname);
    }
    cursor.reset();
    std::sort(expected.begin(), expected.end());
    std::sort(actual.begin(), actual.end());

    if (actual.size() != expected.size()) {
        std::runtime_error err("actual.size() != expected.size()");
        BOOST_THROW_EXCEPTION(err);
    }

    bool has_error = false;
    for (auto i = 0u; i < actual.size(); i++) {
        if (actual.at(i) != expected.at(i)) {
            has_error = true;
            std::cout << "Error at (" << i << "), expected: " << expected.at(i)
                                                << "actual: " << actual.at(i) << std::endl;
        }
    }
    if (has_error) {
        std::runtime_error err("bad metadata query results");
        BOOST_THROW_EXCEPTION(err);
    }
}

int main(int argc, const char** argv) {
    int retcode = 0;
    try {
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
        aku_initialize(nullptr, nullptr);

        u32 compression_threshold = 5;
        u64 windowsize = 1;
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

        const std::vector<std::string> allseries = {
            "cpu key=0",
            "cpu key=1",
            "cpu key=2",
            "cpu key=3",
            "cpu key=4",
            "cpu key=5",
        };

        const std::vector<std::string> evenseries = {
            "cpu key=0",
            "cpu key=2",
            "cpu key=4",
        };

        const std::vector<std::string> oddseries = {
            "cpu key=1",
            "cpu key=3",
            "cpu key=5",
        };

        const std::vector<std::string> noseries;

        const char* include_odd  = R"({"key": [1, 3, 5] })";
        const char* include_even = R"({"key": [0, 2, 4] })";

        {
            // In this stage all data should be cached inside the the sequencer

            // Query all metadata
            query_metadata(&storage, "", "", allseries);
            // Query by metric
            query_metadata(&storage, "mem", "",             noseries);
            query_metadata(&storage, "cpu", "",             allseries);
            // Query by metric and key
            query_metadata(&storage, "cpu", include_odd,    oddseries);
            query_metadata(&storage, "cpu", include_even,   evenseries);

            // Read in forward direction
            query_subset(&storage, "20150101T000000", "20150101T000015", false, false, allseries);
            // Read in backward direction, result-set shouldn't be empty
            query_subset(&storage, "20150101T000000", "20150101T000020", true, false, allseries);
            // Try to read only half of the data-points in forward direction
            query_subset(&storage, "20150101T000005", "20150101T000015", false, false, allseries);
            // Try to read only half of the data-points in backward direction
            query_subset(&storage, "20150101T000005", "20150101T000015", true, false, allseries);
            query_subset(&storage, "20150101T000000", "20150101T000015", true, false, evenseries);
            query_subset(&storage, "20150101T000000", "20150101T000015", true, false, oddseries);

            storage.close();
        }

        {
            // Database is reopened. At this stage everything should be readable in both directions.
            storage.open();

            // Query all metadata
            query_metadata(&storage, "", "", allseries);
            // Query by metric
            query_metadata(&storage, "mem", "",             noseries);
            query_metadata(&storage, "cpu", "",             allseries);
            // Query by metric and key
            query_metadata(&storage, "cpu", include_odd,    oddseries);
            query_metadata(&storage, "cpu", include_even,   evenseries);

            query_subset(&storage, "20150101T000000", "20150101T000020", false, false, allseries);
            query_subset(&storage, "20150101T000000", "20150101T000020", true, false,  allseries);

            // Filter by timestamp
            query_subset(&storage, "20150101T000005", "20150101T000015", false, false, allseries);
            query_subset(&storage, "20150101T000005", "20150101T000015", true, false,  allseries);

            // Filter out BLOBs
            query_subset(&storage, "20150101T000000", "20150101T000020", true, false,  evenseries);
            query_subset(&storage, "20150101T000000", "20150101T000020", false, false, evenseries);
            // Filter out numeric values
            query_subset(&storage, "20150101T000000", "20150101T000020", true, false,  oddseries);
            query_subset(&storage, "20150101T000000", "20150101T000020", false, false, oddseries);

            storage.close();
        }

        {
            storage.open();
            // Add some data
            DataPoint newpoints[] = {
                { "20150101T000020.000000000", "cpu key=2", 2.0 },
                { "20150101T000021.000000000", "cpu key=3", 2.1 },
                { "20150101T000022.000000000", "cpu key=4", 2.2 },
                { "20150101T000023.000000000", "cpu key=5", 2.3 },
            };
            for (int i = 0; i < 4; i++) {
                TEST_DATA.push_back(newpoints[i]);
                add_element(&storage, newpoints[i]);
            }

            query_subset(&storage, "20150101T000020", "20150101T000025", false, false, allseries);
            query_subset(&storage, "20150101T000020", "20150101T000025", true, false, allseries);
            query_subset(&storage, "20150101T000000", "20150101T000020", true, false, allseries);
            query_subset(&storage, "20150101T000000", "20150101T000024", true, false, evenseries);
            query_subset(&storage, "20150101T000000", "20150101T000024", true, false, oddseries);

            storage.close();
        }

        {
            storage.open();

            // All new data should be readable
            query_subset(&storage, "20150101T000000", "20150101T000024", false, false, allseries);
            query_subset(&storage, "20150101T000000", "20150101T000024", true, false,  allseries);

            // Filter by timestamp
            query_subset(&storage, "20150101T000005", "20150101T000015", false, false, allseries);
            query_subset(&storage, "20150101T000005", "20150101T000015", true, false,  allseries);

            // Filter out BLOBs
            query_subset(&storage, "20150101T000000", "20150101T000024", true, false,  evenseries);
            query_subset(&storage, "20150101T000000", "20150101T000024", false, false, evenseries);

            // Filter out numeric values
            query_subset(&storage, "20150101T000000", "20150101T000024", true, false,  oddseries);
            query_subset(&storage, "20150101T000000", "20150101T000024", false, false, oddseries);

            // Add new series name
            DataPoint newpoint = { "20150101T000023.000000000", "cpu key=5 xxx=1", 23 };
            add_element(&storage, newpoint);

            const std::vector<std::string> newodds = {
                "cpu key=1",
                "cpu key=3",
                "cpu key=5",
                "cpu key=5 xxx=1",
            };

            // Query by metric and key
            query_metadata(&storage, "cpu", include_odd, newodds);

            storage.close();
        }

        {
            storage.open();

            // new metadata should be readable
            const std::vector<std::string> newodds = {
                "cpu key=1",
                "cpu key=3",
                "cpu key=5",
                "cpu key=5 xxx=1",
            };

            // Query by metric and key
            query_metadata(&storage, "cpu", include_odd,    newodds);
            query_metadata(&storage, "cpu", include_even,   evenseries);

            storage.close();
        }

        {
            storage.open();
            std::vector<DataPoint> exppoints = {
                { "20150101T000020.000000000", "cpu key=2", 2.0 },
                { "20150101T000021.000000000", "cpu key=3", 2.1 },
                { "20150101T000022.000000000", "cpu key=4", 2.2 },
                { "20150101T000023.000000000", "cpu key=5", 2.3 },
            };
            std::vector<DataPoint> newpoints = {
                { "20150101T000024.000000000", "cpu key=1", 2.4 },
                { "20150101T000025.000000000", "cpu key=2", 2.5 },
                { "20150101T000026.000000000", "cpu key=3", 2.6 },
                { "20150101T000027.000000000", "cpu key=4", 2.7 },
                { "20150101T000028.000000000", "cpu key=5", 2.8 },
                { "20150101T000029.000000000", "cpu key=1", 2.8 },
            };
            std::vector<std::string> ids = {
                "cpu key=1",
                "cpu key=2",
                "cpu key=3",
                "cpu key=4",
                "cpu key=5",
            };
        }

        std::cout << "OK!" << std::endl;

        storage.delete_all();
    } catch (...) {
        std::cout << boost::current_exception_diagnostic_information() << std::endl;
        retcode = -1;
    }
    return retcode;
}
