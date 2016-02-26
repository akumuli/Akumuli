#pragma once

#include <functional>
#include <memory>
#include <vector>

namespace Akumuli {

/** Very basic signal handler.
  * Catches only SIGINT.
  */
struct SignalHandler {
    typedef std::function<void()> Func;

    std::vector<std::pair<Func, int>> handlers_;

    SignalHandler();

    void add_handler(Func cb, int id);

    std::vector<int> wait();
};
}
