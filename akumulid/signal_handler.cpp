#include "signal_handler.h"
#include <iostream>
#include <signal.h>
#include <boost/exception/all.hpp>

namespace Akumuli {

SignalHandler::SignalHandler()
{
}

void SignalHandler::add_handler(std::function<void()> fn, int id) {
    handlers_.push_back(std::make_pair(fn, id));
}

static void sig_handler(int signo) {
    if (signo == SIGINT) {
        std::cout << "SIGINT catched!" << std::endl;
    }
}

std::vector<int> SignalHandler::wait() {
    if (signal(SIGINT, &sig_handler) == SIG_ERR) {
        std::runtime_error error("`signal` error");
        BOOST_THROW_EXCEPTION(error);
    }
    pause();
    std::vector<int> ids;
    for (auto pair: handlers_) {
        pair.first();
        ids.push_back(pair.second);
    }
    handlers_.clear();
    return ids;
}

}

