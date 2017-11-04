#include "status_util.h"
#include <stdexcept>
#include <boost/exception/all.hpp>

namespace Akumuli {

static const char* g_error_messages[] = {
    "OK",
    "no data",
    "not enough memory",
    "device is busy",
    "not found",
    "bad argument",
    "overflow",
    "invalid data",
    "unknown error",
    "late write",
    "not implemented",
    "query parsing error",
    "anomaly detector can't work with negative values",
    "merge required",
    "attempt to perform operation on closed device",
    "timeout",
    "retry required",
    "access denied",
    "operation not permitted",
    "resource is not available",
    "high cardinality, lower cardinality required",
    "regullar series expected",
    "missing data not supported",
    "unknown error code"
};

const char* StatusUtil::c_str(aku_Status error_code) {
    if (error_code >= 0 && error_code < AKU_EMAX_ERROR) {
        return g_error_messages[error_code];
    }
    return g_error_messages[AKU_EMAX_ERROR];
}

std::string StatusUtil::str(aku_Status status) {
    return c_str(status);
}

void StatusUtil::throw_on_error(aku_Status status) {
    if (status != AKU_SUCCESS) {
        BOOST_THROW_EXCEPTION(std::runtime_error(c_str(status)));
    }
}


}  // namespace
