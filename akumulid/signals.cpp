#include "signals.h"
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
        sigaddset(&sset, s);
    }
    int signo = 0;
    if (sigwait(sset, &signo) != 0) {
        std::runtime_error error("`sigwait` error");
        BOOST_THROW_EXCEPTION(error);
    }
    for (auto fn: handlers_) {
        fn();
    }
}

}

