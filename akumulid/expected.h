#pragma once
#include <exception>
#include <stdexcept>

namespace Akumuli {

/** Optional type. Can contain value or error.
  */
template <class Target> class Expected {
    union {
        std::exception_ptr except_;
        Target             value_;
    };
    bool is_set_;

public:
    //! Value c-tor
    Expected(Target const& t)
        : value_(t)
        , is_set_(true) {}

    //! Copy expected value
    Expected(Expected<Target> const& t)
        : is_set_(t.is_set_) {
        if (is_set_) {
            value_ = t.value_;
        } else {
            except_ = t.except_;
        }
    }

    //! Assignment
    Expected<Target>& operator=(Expected<Target> const& t) {
        if (&t == this) {
            return *this;
        }
        is_set_ = t.is_set_;
        if (is_set_) {
            value_ = t.value_;
        } else {
            except_ = t.except_;
        }
        return *this;
    }

    ~Expected() {
        if (is_set_) {
            (&value_)->~Target();
        } else {
            (&except_)->~exception_ptr();
        }
    }

    //! Construct from exception ptr
    Expected(std::exception_ptr&& ptr)
        : except_(std::move(ptr))
        , is_set_(false) {}

    //! Construct from any exception
    template <class E> static Expected<Target> from(E const& e) {
        return Expected<Target>(std::make_exception_ptr(e));
    }

    //! Get value or throw
    const Target& get() const {
        if (!is_set_) {
            std::rethrow_exception(except_);
        }
        return value_;
    }

    //! Check for error
    bool ok() const { return is_set_; }

    //! Extract error from value
    template <class Exception> bool unpack_error(Exception* err) {
        if (is_set_) {
            return false;
        } else {
            try {
                std::rethrow_exception(except_);
            } catch (Exception const& e) {
                *err = e;
            }
        }
        return true;
    }
};
}
