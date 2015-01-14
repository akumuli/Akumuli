#include "logger.h"

namespace Akumuli {

static log4cxx::LoggerPtr s_common_logger_ = log4cxx::Logger::getLogger("main");

Formatter::Formatter()
    : sink_(NONE)
    , buffer_(nullptr)
{
}

Formatter::~Formatter() {
    switch (sink_) {
    case LOGGER_INFO:
        LOG4CXX_INFO(logger_, str_.str());
        break;
    case LOGGER_ERROR:
        if (!buffer_->empty()) {
            LOG4CXX_TRACE(logger_, "=Begin=trace=======================================================");
            for(auto msg: *buffer_) {
                LOG4CXX_TRACE(logger_, msg);
            }
            LOG4CXX_TRACE(logger_, "==========================================================End=trace=");
        }
        LOG4CXX_ERROR(logger_, str_.str());
        break;
    case BUFFER:
        buffer_->push_back(str_.str());
    case NONE:
    break;
    };
}

void Formatter::set_info_sink(log4cxx::LoggerPtr logger) {
    sink_ = LOGGER_INFO;
    logger_ = logger;
}

void Formatter::set_trace_sink(boost::circular_buffer<std::string> *buffer) {
    sink_ = BUFFER;
    buffer_ = buffer;
}

void Formatter::set_error_sink(log4cxx::LoggerPtr logger, boost::circular_buffer<std::string> *buffer) {
    sink_ = LOGGER_ERROR;
    logger_ = logger;
    buffer_ = buffer;
}

Logger::Logger(const char* log_name, int depth)
    : trace_(depth)
    , plogger_(log4cxx::Logger::getLogger(log_name))
{
}

Formatter&& Logger::trace(Formatter&& fmt) {
    fmt.set_trace_sink(&trace_);
    return std::move(fmt);
}

Formatter&& Logger::info(Formatter&& fmt) {
    fmt.set_info_sink(plogger_);
    return std::move(fmt);
}

Formatter&& Logger::error(Formatter&& fmt) {
    fmt.set_error_sink(plogger_, &trace_);
    return std::move(fmt);
}

}  // namespace
