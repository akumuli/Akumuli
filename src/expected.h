#pragma once
#include <exception>
#include <stdexcept>

namespace Akumuli {

/** Optional type. Can contain value or error.
  */
template<class Target>
class Expected {
    union {
        std::exception_ptr except_;
        Target value_;
    };
    bool is_set_;
public:
    Expected(Expected<Target> const& t)
        : is_set_(t.is_set_)
    {
        if (is_set_) {
            value_ = t.value_;
        } else {
            except_ = t.except_;
        }
    }

    ~Expected() {
    }

    template<class E>
    Expected(E const& e) : except_(std::make_exception_ptr(e)), is_set_(false) {}

    Expected(std::exception_ptr&& ptr) : except_(std::move(ptr)), is_set_(false) {}

    Expected(Target const& t) : value_(t), is_set_(true) {}

    const Target& get() const {
        if (!is_set_) {
            std::rethrow_exception(except_);
        }
        return value_;
    }

    template<class Exception>
    bool unpack_error(Exception *err) {
        if (is_set_) {
            return false;
        } else {
            try {
                std::rethrow_exception(except_);
            } catch(Exception const& e) {
                *err = e;
            }
        }
        return true;
    }
};

}
