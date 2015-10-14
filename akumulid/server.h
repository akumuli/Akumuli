#pragma once
#include "signal_handler.h"

namespace Akumuli {
    //! Server interface
    struct Server {
        virtual ~Server() = default;
        virtual void start(SignalHandler* sig_handler, int id) = 0;
    };
}
