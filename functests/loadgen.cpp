#include <iostream>
#include <boost/program_options.hpp>
#include <boost/exception/all.hpp>

namespace po = boost::program_options;

/**
 * @brief Data generator
 */
class Generator {
public:
};

int main(int argc, char** argv) {
    po::options_description desc("Options");
    desc.add_options()
        ("help", "Print help messages")
        ("host", "Akumuli host IP")
        ("nmsg", "Number of messages to send")
        ("ncon", "Number of connections")
        ("cardinality", "Number of unique sereis");

    po::variables_map vm;
    try
    {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        if (vm.count("help")) {
            std::cout << "Basic Command Line Parameter App" << std::endl
                      << desc << std::endl;
            return 0;
        }

        po::notify(vm);
    } catch (...) {
        std::cout << boost::current_exception_diagnostic_information() << std::endl;
    }
    return 0;
}
