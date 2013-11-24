#include "util.h"

namespace Akumuli
{

AprException::AprException(apr_status_t status, const char* message)
    : std::runtime_error(message)
    , status_(status)
{
}

std::ostream& operator << (std::ostream& str, AprException const& e) {
    str << "Error: " << e.what() << ", status: " << e.status_ << ".";
    return str;
}

void apr_throw_if_error(apr_status_t status) {
    if (status != APR_SUCCESS) {
        char buf[0x100];
        const char* err = apr_strerror(status, buf, 1024);
        throw AprException(status, err);
    }
}

AprStatusChecker::AprStatusChecker()
    : count(0)
    , status(APR_SUCCESS)
{
}

AprStatusChecker AprStatusChecker::operator = (apr_status_t st) {
    status = st;
    apr_throw_if_error(st);
    count++;
    return *this;
}

}
