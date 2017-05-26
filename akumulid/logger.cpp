#include "logger.h"
#include "log4cxx/propertyconfigurator.h"

namespace Akumuli {

static log4cxx::LoggerPtr s_common_logger_ = log4cxx::Logger::getLogger("main");

Formatter::Formatter()
    : sink_(NONE)
{
}

Formatter::~Formatter() {
    switch (sink_) {
    case LOGGER_INFO:
        LOG4CXX_INFO(logger_, str_.str());
        break;
    case LOGGER_ERROR:
        LOG4CXX_ERROR(logger_, str_.str());
        break;
    case LOGGER_TRACE:
        LOG4CXX_TRACE(logger_, str_.str());
        break;
    case NONE:
    break;
    };
}

void Formatter::set_info_sink(log4cxx::LoggerPtr logger) {
    sink_ = LOGGER_INFO;
    logger_ = logger;
}

void Formatter::set_trace_sink(log4cxx::LoggerPtr logger) {
    sink_ = LOGGER_TRACE;
    logger_ = logger;
}

void Formatter::set_error_sink(log4cxx::LoggerPtr logger) {
    sink_ = LOGGER_ERROR;
    logger_ = logger;
}

Logger::Logger(const char* log_name)
    : plogger_(log4cxx::Logger::getLogger(log_name))
{
}

Logger::Logger(std::string log_name)
    : plogger_(log4cxx::Logger::getLogger(log_name))
{
}

Formatter&& Logger::trace(Formatter&& fmt) {
    fmt.set_trace_sink(plogger_);
    return std::move(fmt);
}

Formatter&& Logger::info(Formatter&& fmt) {
    fmt.set_info_sink(plogger_);
    return std::move(fmt);
}

Formatter&& Logger::error(Formatter&& fmt) {
    fmt.set_error_sink(plogger_);
    return std::move(fmt);
}

void Logger::init(std::string path) {
    log4cxx::File file(path);
    log4cxx::PropertyConfigurator::configure(file);
}

}  // namespace
