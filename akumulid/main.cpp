#include "akumuli.h"
#include "tcp_server.h"
#include "udp_server.h"
#include "httpserver.h"
#include "utility.h"
#include "query_results_pooler.h"
#include "signal_handler.h"

#include <iostream>
#include <fstream>
#include <regex>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <apr_errno.h>

#include <wordexp.h>
#include <unistd.h>

namespace po=boost::program_options;
using namespace Akumuli;


//! Default configuration for `akumulid`
const char* DEFAULT_CONFIG = R"(# akumulid configuration file (generated automatically).

# path to database files.  Default values is  ~/.akumuli.
path=~/.akumuli

# Number of volumes used  to store data.  Each volume  is
# 4Gb in size and  allocated beforehand. To change number
# of  volumes  they  should  change  `nvolumes`  value in
# configuration and restart daemon.
nvolumes=4

# Sliding window  width. Can  be specified in nanoseconds
# (unit  of  measure  is  not  specified),  seconds  (s),
# milliseconds (ms),  microseconds (us) or minutes (min).
#
# Examples:
#
# window=10s
# window=500us
# window=1000   (in this case sliding window will be 1000
#                nanoseconds long)
window=10s

# Number of data  points stored in one  compressed chunk.
# Akumuli stores data  in chunks.  If chunks is too large
# decompression can be slow; in opposite case compression
# can  be less  effective.  Good compression_threshold is
# about 1000. In this case chunk size will be around 4Kb.
compression_threshold=1000

# Durability level can  be set  to  max  or  min.  In the
# first case  durability will  be maximal but speed won't
# be  optimal.  If durability is  set  to min write speed
# will be better.
durability=max

# This parameter  can  be used to  emable huge  pages for
# data volumes.  This can speed up searching  and writing
# process a bit. Setting  this  option can't  do any harm
# except performance loss in some circumstances.
huge_tlb = 0

# Max cache capacity in bytes.  Akumuli  caches  recently
# used chunks  in memory. This  parameter can  be used to
# define size of this cache (default value: 512Mb).
max_cache_size=536870912


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

# Logging configuration
# This is just a log4cxx configuration without any modifications

log4j.rootLogger=all, file
log4j.appender.file=org.apache.log4j.DailyRollingFileAppender
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %c [%p] %l %m%n
log4j.appender.file.filename=/tmp/akumuli.log
log4j.appender.file.datePattern='.'yyyy-MM-dd

)";


struct ServerSettings {
    int port;
    int nworkers;
};


//! Container class for configuration related functions
struct ConfigFile {
    typedef boost::property_tree::ptree PTree;

    static boost::filesystem::path default_config_path() {
        auto path2cfg = boost::filesystem::path(getenv("HOME"));
        path2cfg /= ".akumulid";
        return path2cfg;
    }

    static void init_config(boost::filesystem::path path) {
        if (boost::filesystem::exists(path)) {
            std::runtime_error err("configuration file already exists");
            BOOST_THROW_EXCEPTION(err);
        }
        std::ofstream stream(path.c_str());
        stream << DEFAULT_CONFIG << std::endl;
        stream.close();
    }

    static PTree read_config_file(boost::filesystem::path file_path) {

        if (!boost::filesystem::exists(file_path)) {
            std::stringstream fmt;
            fmt << "can't read config file `" << file_path << "`";
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        PTree conf;
        boost::property_tree::ini_parser::read_ini(file_path.c_str(), conf);
        return conf;
    }

    static boost::filesystem::path get_path(PTree conf) {
        std::string path = conf.get<std::string>("path");
        wordexp_t we;
        int err = wordexp(path.c_str(), &we, 0);
        if (err) {
            std::stringstream fmt;
            fmt << "invalid path: `" << path << "`";
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }
        if (we.we_wordc != 1) {
            std::stringstream fmt;
            fmt << "expansion error, path: `" << path << "`";
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }
        path = std::string(we.we_wordv[0]);
        wordfree(&we);
        auto result = boost::filesystem::path(path);
        return result;
    }

    static uint64_t get_cache_size(PTree conf) {
        return conf.get<uint64_t>("max_cache_size");
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

    static bool get_huge_tlb(PTree conf) {
        return conf.get<bool>("huge_tlb");
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

    static ServerSettings get_http_server(PTree conf) {
        ServerSettings settings;
        settings.port = conf.get<int>("HTTP.port");
        settings.nworkers = -1;
        return settings;
    }

    static ServerSettings get_udp_server(PTree conf) {
        ServerSettings settings;
        settings.port = conf.get<int>("UDP.port");
        settings.nworkers = conf.get<int>("UDP.pool_size");
        return settings;
    }

    static ServerSettings get_tcp_server(PTree conf) {
        ServerSettings settings;
        settings.port = conf.get<int>("TCP.port");
        settings.nworkers = conf.get<int>("TCP.pool_size");
        return settings;
    }
};


/** Help message used in CLI. It contains simple markdown formatting.
  * `rich_print function should be used to print this message.
  */
const char* CLI_HELP_MESSAGE = R"(`akumulid` - time-series database daemon

**SYNOPSIS**
        akumulid

        akumulid --help

        akumulid --init

        akumulid --create

        akumuild --delete

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

        **create**
            generate database files in `~/.akumuli` folder

        **delete**
            delete database files in `~/.akumuli` folder

        **(empty)**
            run server

)";


//! Format text for console. `plain_text` flag removes formatting.
std::string cli_format(std::string dest) {

    bool plain_text = !isatty(STDOUT_FILENO);

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
void rich_print(const char* msg) {

    std::stringstream stream(const_cast<char*>(msg));
    std::string dest;

    while(std::getline(stream, dest)) {
        std::cout << cli_format(dest) << std::endl;
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


/** Create database if database not exists.
  */
void create_db_files(const char* path,
                     int32_t nvolumes)
{
    auto full_path = boost::filesystem::path(path) / "db.akumuli";
    if (!boost::filesystem::exists(full_path)) {
        auto status = aku_create_database("db", path, path, nvolumes,
                                          &static_logger);
        if (status != APR_SUCCESS) {
            char buffer[1024];
            apr_strerror(status, buffer, 1024);
            std::stringstream fmt;
            fmt << "can't create database: " << buffer;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        } else {
            std::stringstream fmt;
            fmt << "**OK** database created, path: `" << path << "`";
            std::cout << cli_format(fmt.str()) << std::endl;
        }
    } else {
        std::stringstream fmt;
        fmt << "**ERROR** database file already exists";
        std::cout << cli_format(fmt.str()) << std::endl;
    }
}

/** Read configuration file and run server.
  * If config file can't be found - report error.
  */
void cmd_run_server() {
    auto config_path = ConfigFile::default_config_path();

    auto config                 = ConfigFile::read_config_file(config_path);
    auto window                 = ConfigFile::get_window(config);
    auto durability             = ConfigFile::get_durability(config);
    auto path                   = ConfigFile::get_path(config);
    auto compression_threshold  = ConfigFile::get_compression_threshold(config);
    auto huge_tlb               = ConfigFile::get_huge_tlb(config);
    auto cache_size             = ConfigFile::get_cache_size(config);
    auto http_conf              = ConfigFile::get_http_server(config);
    auto tcp_conf               = ConfigFile::get_tcp_server(config);
    auto udp_conf               = ConfigFile::get_udp_server(config);

    auto full_path = boost::filesystem::path(path) / "db.akumuli";

    auto connection = std::make_shared<AkumuliConnection>(full_path.c_str(),
                                                          huge_tlb,
                                                          durability,
                                                          compression_threshold,
                                                          window,
                                                          cache_size);

    auto pipeline = std::make_shared<IngestionPipeline>(connection, AKU_LINEAR_BACKOFF);

    SignalHandler sighandler({SIGINT});

    auto udp_server = std::make_shared<UdpServer>(pipeline, udp_conf.nworkers, udp_conf.port);

    auto tcp_server = std::make_shared<TcpServer>(pipeline, tcp_conf.nworkers, tcp_conf.port);

    auto qproc = std::make_shared<QueryProcessor>(connection, 1000);
    auto httpserver = std::make_shared<Http::HttpServer>(http_conf.port, qproc);

    udp_server->start();
    std::cout << cli_format("**OK** UDP  server started, port: ") << udp_conf.port << std::endl;
    tcp_server->start(&sighandler);
    std::cout << cli_format("**OK** TCP  server started, port: ") << tcp_conf.port << std::endl;
    httpserver->start();
    std::cout << cli_format("**OK** HTTP server started, port: ") << http_conf.port << std::endl;

    sighandler.wait();

    udp_server->stop();
    std::cout << cli_format("**OK** UDP  server stopped") << std::endl;
    tcp_server->stop();
    std::cout << cli_format("**OK** TCP  server stopped") << std::endl;
    httpserver->stop();
    std::cout << cli_format("**OK** HTTP server stopped") << std::endl;
}

/** Create database command.
  */
void cmd_create_database() {
    auto config_path = ConfigFile::default_config_path();

    auto config     = ConfigFile::read_config_file(config_path);
    auto path       = ConfigFile::get_path(config);
    auto volumes    = ConfigFile::get_nvolumes(config);

    create_db_files(path.c_str(), volumes);
}

void cmd_delete_database() {
    auto config_path = ConfigFile::default_config_path();

    auto config     = ConfigFile::read_config_file(config_path);
    auto path       = ConfigFile::get_path(config);

    auto full_path = boost::filesystem::path(path) / "db.akumuli";
    if (boost::filesystem::exists(full_path)) {
        auto status = aku_remove_database(full_path.c_str(), &static_logger);
        if (status != APR_SUCCESS) {
            char buffer[1024];
            apr_strerror(status, buffer, 1024);
            std::stringstream fmt;
            fmt << "can't delete database: " << buffer;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        } else {
            std::stringstream fmt;
            fmt << "**OK** database at `" << path << "` deleted";
            std::cout << cli_format(fmt.str()) << std::endl;
        }
    } else {
        std::stringstream fmt;
        fmt << "**ERROR** database file doesn't exists";
        std::cout << cli_format(fmt.str()) << std::endl;
    }
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

    try {
        // Init logger
        auto path = ConfigFile::default_config_path();
        if (boost::filesystem::exists(path)) {
            Logger::init(path.c_str());
        }

        po::options_description cli_only_options;
        cli_only_options.add_options()
                ("help", "Produce help message")
                ("create", "Create database")
                ("delete", "Delete database")
                ("init", "Create default configuration");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
        po::notify(vm);

        if (vm.count("help")) {
            rich_print(CLI_HELP_MESSAGE);
            exit(EXIT_SUCCESS);
        }

        if (vm.count("init")) {
            ConfigFile::init_config(path);

            std::stringstream fmt;
            fmt << "**OK** configuration file created at: `" << path << "`";
            std::cout << cli_format(fmt.str()) << std::endl;
            exit(EXIT_SUCCESS);
        }

        if (vm.count("create")) {
            cmd_create_database();
            exit(EXIT_SUCCESS);
        }

        if (vm.count("delete")) {
            cmd_delete_database();
            exit(EXIT_SUCCESS);
        }

        cmd_run_server();

    } catch(const std::exception& e) {
        std::stringstream fmt;
        fmt << "**FAILURE** " << e.what();
        std::cerr << cli_format(fmt.str()) << std::endl;
        exit(EXIT_FAILURE);
    }

    exit(EXIT_SUCCESS);
}

