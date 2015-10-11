#pragma once

#include <functional>
#include <memory>
#include <vector>

namespace Akumuli {

/** Simple signal handler
  */
struct SignalHandler {
    typedef std::function<void()> Func;

    std::vector<Func> handlers_;
    std::vector<int> signals_;

    SignalHandler(std::vector<int> s);

    void add_handler(std::function<void()> cb);

    void wait();
};

}
