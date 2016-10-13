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

const i64 AKU_TEST_PAGE_SIZE  = 0x1000000;

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

    static u64 get_cache_size(PTree conf) {
        return conf.get<u64>("max_cache_size");
    }

    static int get_nvolumes(PTree conf) {
        return conf.get<int>("nvolumes");
    }

    static ServerSettings get_http_server(PTree conf) {
        ServerSettings settings;
        settings.name = "HTTP";
        settings.port = conf.get<int>("HTTP.port");
        settings.nworkers = -1;
        return settings;
    }

    static ServerSettings get_udp_server(PTree conf) {
        ServerSettings settings;
        settings.name = "UDP";
        settings.port = conf.get<int>("UDP.port");
        settings.nworkers = conf.get<int>("UDP.pool_size");
        return settings;
    }

    static ServerSettings get_tcp_server(PTree conf) {
        ServerSettings settings;
        settings.name = "TCP";
        settings.port = conf.get<int>("TCP.port");
        settings.nworkers = conf.get<int>("TCP.pool_size");
        return settings;
    }

    static std::vector<ServerSettings> get_server_settings(PTree conf) {
        //TODO: this should be generic
        std::vector<ServerSettings> result = {
            get_tcp_server(conf),
            get_udp_server(conf),
            get_http_server(conf),
        };
        return result;
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
                     i32 nvolumes,
                     u64 page_size=0)
{
    auto full_path = boost::filesystem::path(path) / "db.akumuli";
    if (!boost::filesystem::exists(full_path)) {
        apr_status_t status = APR_SUCCESS;
        status = aku_create_database_ex("db", path, path, nvolumes, page_size);
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
    auto config_path            = ConfigFile::default_config_path();
    auto config                 = ConfigFile::read_config_file(config_path);
    auto path                   = ConfigFile::get_path(config);
    auto ingestion_servers      = ConfigFile::get_server_settings(config);
    auto full_path              = boost::filesystem::path(path) / "db.akumuli";
    auto connection             = std::make_shared<AkumuliConnection>(full_path.c_str());
    auto qproc                  = std::make_shared<QueryProcessor>(connection, 1000);

    SignalHandler sighandler;
    int srvid = 0;
    std::map<int, std::string> srvnames;
    for(auto settings: ingestion_servers) {
        auto srv = ServerFactory::instance().create(connection, qproc, settings);
	assert(srv != nullptr);
        srvnames[srvid] = settings.name;
        srv->start(&sighandler, srvid++);
        std::cout << cli_format("**OK** ") << settings.name << " server started, port: " << settings.port << std::endl;
    }

    auto srvids = sighandler.wait();

    for(int id: srvids) {
        std::cout << cli_format("**OK** ") << srvnames[id] << " server stopped" << std::endl;
    }
}

/** Create database command.
  */
void cmd_create_database(bool test_db=false) {
    auto config_path = ConfigFile::default_config_path();

    auto config      = ConfigFile::read_config_file(config_path);
    auto path        = ConfigFile::get_path(config);
    auto volumes     = ConfigFile::get_nvolumes(config);

    create_db_files(path.c_str(), volumes, test_db ? AKU_TEST_PAGE_SIZE : 0);
}

void cmd_delete_database() {
    auto config_path = ConfigFile::default_config_path();

    auto config     = ConfigFile::read_config_file(config_path);
    auto path       = ConfigFile::get_path(config);

    auto full_path = boost::filesystem::path(path) / "db.akumuli";
    if (boost::filesystem::exists(full_path)) {
        // TODO: don't delete database if it's not empty
        // FIXME: add command line argument --force to delete nonempty database
        auto status = aku_remove_database(full_path.c_str(), true);
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

    try {
        aku_initialize(&panic_handler, &static_logger);

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
                ("CI", "Create database for CI environment (for testing)")
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
            cmd_create_database(false);
            exit(EXIT_SUCCESS);
        }

        if (vm.count("CI")) {
            cmd_create_database(true);
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
    } catch(...) {
        std::stringstream fmt;
        fmt << "**FAILURE** " << boost::current_exception_diagnostic_information(true);
        std::cerr << cli_format(fmt.str()) << std::endl;
        exit(EXIT_FAILURE);
    }

    exit(EXIT_SUCCESS);
}

