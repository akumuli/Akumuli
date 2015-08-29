#include "tcp_server.h"
#include "httpserver.h"
#include "utility.h"
#include "query_results_pooler.h"

#include <iostream>
#include <fstream>
#include <regex>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <apr_errno.h>

namespace po=boost::program_options;
using namespace Akumuli;

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
               int32_t nvolumes,
               uint32_t compression_threshold,
               uint64_t window_size,
               uint32_t max_cache_size)
{
    auto status = aku_create_database(name, path, path, nvolumes,
                                      compression_threshold,
                                      window_size,
                                      max_cache_size,
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

int main(int argc, char** argv) {
    aku_initialize(nullptr);

    po::options_description cli_only_options;
    cli_only_options.add_options()
            ("help",                                "Produce help message")
            ("create",                              "Create database")
            ("name", po::value<std::string>(),      "Database name (create)")
            ("nvolumes", po::value<int32_t>(),      "Number of volumes to create (create)")
            ("window", po::value<std::string>(),    "Window size (create)")
            ;

    po::options_description generic_options;
    generic_options.add_options()
            ("path", po::value<std::string>(),      "Path to database files")
            ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
    auto path2cfg = boost::filesystem::path(getenv("HOME"));
    path2cfg /= ".akumulid";
    std::fstream config_file(path2cfg.c_str());
    po::store(po::parse_config_file(config_file, generic_options, true), vm);  // allow_unregistered=true
    po::notify(vm);
    if (vm.count("help")) {
        std::cout << cli_only_options << std::endl;
        return 0;
    }
    if (!vm.count("path")) {
        std::cout << "Incomplete configuration, path required" << std::endl;
        std::cout << generic_options << std::endl;
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
        std::string window = vm["window"].as<std::string>();
        create_db(name.c_str(), path.c_str(), nvol, 10000, str2unixtime(window), 100000);  // TODO: use correct numbers
    }

    run_server(path);

    return 0;
}

