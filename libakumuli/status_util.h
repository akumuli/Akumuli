#pragma once

#include "akumuli_def.h"  // for aku_Status enum
#include <string>

namespace Akumuli {


//! Error handling utility class
struct StatusUtil {
    //! Convert error code (aku_Status) to error message
    static const char* c_str(aku_Status error_code);

    //! Convert status to std::string
    static std::string str(aku_Status status);

    static void throw_on_error(aku_Status status);
};


}  // namespace
