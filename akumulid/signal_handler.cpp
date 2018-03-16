#include "signal_handler.h"
#include "logger.h"
#include <iostream>
#include <signal.h>
#include <boost/exception/all.hpp>

namespace Akumuli {

static Logger logger("sighandler");

SignalHandler::SignalHandler()
{
}

void SignalHandler::add_handler(std::function<void()> fn, int id) {
    handlers_.push_back(std::make_pair(fn, id));
}

static void sig_handler(int signo) {
    if (signo == SIGINT) {
        logger.info() << "SIGINT handler called";
    } else if (signo == SIGTERM) {
        logger.info() << "SIGTERM handler called";
    }
}

std::vector<int> SignalHandler::wait() {
    if (signal(SIGINT, &sig_handler) == SIG_ERR) {
        logger.error() << "Signal handler error, signal returned SIG_ERR";
        std::runtime_error error("`signal` error");
        BOOST_THROW_EXCEPTION(error);
    }
    if (signal(SIGTERM, &sig_handler) == SIG_ERR) {
        logger.error() << "Signal handler error, signal returned SIG_ERR";
        std::runtime_error error("`signal` error");
        BOOST_THROW_EXCEPTION(error);
    }

    logger.info() << "Waiting for the signals";

    pause();

    logger.info() << "Start calling signal handlers";

    std::vector<int> ids;
    for (auto pair: handlers_) {
        logger.info() << "Calling signal handler " << pair.second;
        pair.first();
        ids.push_back(pair.second);
    }
    handlers_.clear();
    return ids;
}

}

