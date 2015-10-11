#include "signal_handler.h"
#include <iostream>
#include <signal.h>
#include <boost/exception/all.hpp>

namespace Akumuli {

SignalHandler::SignalHandler(std::vector<int> s)
    : signals_(s)
{
}

void SignalHandler::add_handler(std::function<void()> fn) {
    handlers_.push_back(fn);
}

void SignalHandler::wait() {
    sigset_t sset = {};
    sigemptyset(&sset);
    for(auto s: signals_) {
        std::cout << "adding signal " << s << std::endl;
        if (sigaddset(&sset, s) != 0) {
            std::cout << "sigaddset error" << std::endl;
        }
    }
    int signo = 0;
    std::cout << "starting to wait for signals" << std::endl;
    if (sigwait(&sset, &signo) != 0) {
        std::cout << "sigwait error" << std::endl;
        std::runtime_error error("`sigwait` error");
        BOOST_THROW_EXCEPTION(error);
    }
    std::cout << "signal received" << std::endl;
    for (auto fn: handlers_) {
        fn();
    }
}

}

