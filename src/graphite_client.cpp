#include <boost/asio.hpp>
#include <string>
#include <chrono>
#include <cstdlib>

namespace Akumuli {

void push_metric_to_graphite(std::string metric, double value) {
#ifdef DEBUG
    return;
#endif
    // get graphite host
    char* pgraphite = std::getenv("GRAPHITE_HOST");
    std::string graphite_host;
    if (pgraphite) {
        graphite_host = pgraphite;
    }
    // get machine name
    char* phost = std::getenv("HOST");
    std::string host;
    if (phost) {
        host = phost;
    } else {
        phost = std::getenv("HOSTNAME");
        if (phost) {
            host = phost;
        }
    }
    // push data to graphite
    boost::asio::io_service io;
    boost::asio::streambuf buf;
    std::ostream os(&buf);
    auto address = boost::asio::ip::address_v4::from_string(graphite_host);
    boost::asio::ip::tcp::endpoint peer(address, 2003);
    boost::asio::ip::tcp::socket sock(io);
    sock.connect(peer);

    // Create message

    // metric name
    if (!host.empty()) {
        os << "akumuli." << metric << "." << host << " ";
    } else {
        os << "akumuli." << metric << " ";
    }
    // value
    os << value << " ";
    // timestamp
    time_t tm = std::time(0);
    os << tm << "\n";

    // Send

    boost::asio::write(sock, buf);

    sock.shutdown(boost::asio::ip::tcp::socket::shutdown_both);
    sock.close();
}

}
