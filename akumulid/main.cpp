#include "akumuli.h"
#include "tcp_server.h"
#include "httpserver.h"
#include "utility.h"
#include "query_results_pooler.h"

#include <iostream>
#include <fstream>
#include <regex>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <apr_errno.h>

namespace po=boost::program_options;
using namespace Akumuli;


//! Default configuration for `akumulid`
const char* DEFAULT_CONFIG = R"(#Configuration file (generated automatically).

# Main config

# path to main database file
path=~/.akumuli/main.akumuli
# number of volumes, each volume uses 8Gb
nvolumes=32
# sliding window width, all data points older then this wouldn't be recorded
window=10s
# number of data points in each compressed chunk
compression_threshold=1000
# speed-durability tradeoff (can be set to 'max' or 'min' at the moment)
durability=max


# HTTP server config

[HTTP]
# port number
port=8181


# TCP ingestion server config (delete to disable)

[TCP]
# port number
port=8282
# worker pool size
pool_size=1


# UDP ingestion server config (delete to disable)

[UDP]
# port number
port=8383
# worker pool size
pool_size=1
)";


//! Container class for configuration related functions
struct ConfigFile {
    typedef boost::property_tree::ptree PTree;

    static boost::filesystem::path default_config_path() {
        auto path2cfg = boost::filesystem::path(getenv("HOME"));
        path2cfg /= ".akumulid";
        return path2cfg;
    }

    static void init_config(boost::filesystem::path path) {
        std::ofstream stream(path.c_str());
        stream << DEFAULT_CONFIG << std::endl;
        stream.close();
    }

    static PTree read_config_file(boost::filesystem::path file_path) {
        PTree conf;
        boost::property_tree::ini_parser::read_ini(file_path.c_str(), conf);
        return conf;
    }

    static std::string get_path(PTree conf) {
        return conf.get<std::string>("path");
    }

    static int get_window(PTree conf) {
        std::string window = conf.get<std::string>("window");
        int r = 0;
        auto status = aku_parse_duration(window.c_str(), &r);
        if (status != AKU_SUCCESS) {
            throw std::runtime_error("can't parse `window` parameter");
        }
        return r;
    }

    static int get_nvolumes(PTree conf) {
        return conf.get<int>("nvolumes");
    }

    static int get_compression_threshold(PTree conf) {
        return conf.get<int>("compression_threshold");
    }

    static AkumuliConnection::Durability get_durability(PTree conf) {
        std::string m = conf.get<std::string>("durability");
        AkumuliConnection::Durability res;
        if (m == "max") {
            res = AkumuliConnection::MaxDurability;
        } else if (m == "min") {
            res = AkumuliConnection::MaxThroughput;
        } else {
            throw std::runtime_error("unknown durability level");
        }
        return res;
    }

};


/** Help message used in CLI. It contains simple markdown formatting.
  * `rich_print function should be used to print this message.
  */
const char* CLI_HELP_MESSAGE = R"(
**NAME**
        akumulid - time-series database daemon

**SYNOPSIS**
        akumulid

        akumulid --help

        akumulid --init

**DESCRIPTION**
        **akumulid** is a time-series database daemon.
        All configuration can be done via `~/.akumulid` configuration
        file.

**OPTIONS**
        **help**
            produce help message and exit

        **init**
            create  configuration  file at `~/.akumulid`  filled with
            default values and exit.

**CONFIGURATION**
        **path**
            path to database files. Default values is `~/.akumuli/`.

        **window**
            sliding window  width. Can  be specified in nanoseconds
            (unit  of  measure  is  not  specified),  seconds  (s),
            milliseconds (ms),  microseconds (us) or minutes (min).

            Examples:

            **window**=__10s__
            **window**=__500us__
            **window**=__1000__   (in this case sliding window will be 1000
                           nanoseconds long)

        **nvolumes**
            number of volumes used  to store data.  Each volume  is
            `8Gb` in size and  allocated beforehand. To change number
            of  volumes  they  should  change  `nvolumes`  value in
            configuration and restart daemon.

        **compression_threshold**
            number of data  points stored in one  compressed chunk.
            `Akumuli` stores data  in chunks.  If chunks is too large
            decompression can be slow; in opposite case compression
            can  be less  effective.  Good `compression_threshold` is
            about 1000. In this case chunk size will be around 4Kb.

        **durability**
            durability level can  be set  to  `max`  or  `min`.  In the
            first case  durability will  be maximal but speed won't
            be  optimal.  If `durability` is  set  to `min` write speed
            will be better.

)";


//! Format text for console. `plain_text` flag removes formatting.
std::string cli_format(std::string dest, bool plain_text) {

    const char* BOLD = "\033[1m";
    const char* EMPH = "\033[3m";
    const char* UNDR = "\033[4m";
    const char* NORM = "\033[0m";

    auto format = [&](std::string& line, const char* pattern, const char* open) {
        size_t pos = 0;
        int token_num = 0;
        while(pos != std::string::npos) {
            pos = line.find(pattern, pos);
            if (pos != std::string::npos) {
                // match
                auto code = token_num % 2 ? NORM : open;
                line.replace(pos, strlen(pattern), code);
                token_num++;
            }
        }
    };

    if (!plain_text) {
        format(dest, "**", BOLD);
        format(dest, "__", EMPH);
        format(dest, "`",  UNDR);
    } else {
        format(dest, "**", "");
        format(dest, "__", "");
        format(dest, "`",  "");
    }

    return dest;
}

//! Convert markdown subset to console escape codes and print
void rich_print(const char* msg, bool plain_text=true) {

    std::stringstream stream(const_cast<char*>(msg));
    std::string dest;

    while(std::getline(stream, dest)) {
        std::cout << cli_format(dest, plain_text) << std::endl;
    }
}

/** Logger f-n that shuld be used in libakumuli */
static void static_logger(aku_LogLevel tag, const char * msg) {
    static Logger logger = Logger("Main", 32);
    switch(tag) {
    case AKU_LOG_ERROR:
        logger.error() << msg;
        break;
    case AKU_LOG_INFO:
        logger.info() << msg;
        break;
    case AKU_LOG_TRACE:
        logger.trace() << msg;
        break;
    }
}

void create_db(const char* name,
               const char* path,
               int32_t nvolumes)
{
    auto status = aku_create_database(name, path, path, nvolumes,
                                      &static_logger);
    if (status != AKU_SUCCESS) {
        std::cout << "Error creating database" << std::endl;
        char buffer[1024];
        apr_strerror(status, buffer, 1024);
        std::cout << buffer << std::endl;
    } else {
        std::cout << "Database created" << std::endl;
        std::cout << "- path: " << path << std::endl;
        std::cout << "- name: " << name << std::endl;
    }
}

void run_server(std::string path) {

    auto connection = std::make_shared<AkumuliConnection>(path.c_str(),
                                                          false,
                                                          AkumuliConnection::MaxDurability);

    auto tcp_server = std::make_shared<TcpServer>(connection, 4);

    auto qproc = std::make_shared<QueryProcessor>(connection, 1000);

    auto httpserver = std::make_shared<Http::HttpServer>(8877, qproc);

    tcp_server->start();
    httpserver->start();
    tcp_server->wait();
    tcp_server->stop();
    httpserver->stop();
}

uint64_t str2unixtime(std::string t) {
    std::regex regex("(\\d)+\\s?(min|sec|s|m)");
    std::smatch sm;
    std::regex_match(t, sm, regex);
    if (sm.size() != 2) {
        throw std::runtime_error("Bad window size");
    }
    std::string snum = sm[0].str();
    auto num = boost::lexical_cast<uint64_t>(snum);
    if (sm[1].str() == "m" || sm[1].str() == "min") {
        return num * 60;
    }
    return num;
}


/** Panic handler for libakumuli.
  * Shouldn't be called directly, writes error message and
  * writes coredump (this depends on system configuration)
  */
void panic_handler(const char * msg) {
    // write error message
    static_logger(AKU_LOG_ERROR, msg);
    static_logger(AKU_LOG_ERROR, "Terminating (core dumped)");
    // this should generate SIGABORT and triger coredump
    abort();
}


int main(int argc, char** argv) {

    aku_initialize(&panic_handler);

    po::options_description cli_only_options;
    cli_only_options.add_options()
            ("help", "Produce help message")
            ("init", "Create default configuration");


    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
    auto path2cfg = boost::filesystem::path(getenv("HOME"));
    path2cfg /= ".akumulid";
    std::fstream config_file(path2cfg.c_str());
    //po::store(po::parse_config_file(config_file, generic_options, true), vm);  // allow_unregistered=true
    po::notify(vm);
    if (vm.count("help")) {
        rich_print(CLI_HELP_MESSAGE, false);
        return 0;
    }
    if (!vm.count("path")) {
        std::cout << "Incomplete configuration, path required" << std::endl;
        //std::cout << generic_options << std::endl;
        return -1;
    }
    std::string path = vm["path"].as<std::string>();
    if (vm.count("create") != 0) {
        if (vm.count("nvolumes") == 0 || vm.count("name") == 0 || vm.count("window") == 0) {
            std::cout << cli_only_options << std::endl;
            return -1;
        }
        std::string name = vm["name"].as<std::string>();
        int32_t nvol = vm["nvolumes"].as<int32_t>();
        create_db(name.c_str(), path.c_str(), nvol);  // TODO: use correct numbers
    }

    run_server(path);

    return 0;
}

