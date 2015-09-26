#include "logger.h"
#include "log4cxx/propertyconfigurator.h"

namespace Akumuli {

static log4cxx::LoggerPtr s_common_logger_ = log4cxx::Logger::getLogger("main");

namespace details {

CircularBuffer::CircularBuffer(size_t depth)
    : trace_(depth)
{
}

CircularBuffer::CircularBuffer(CircularBuffer&& other)
    : trace_(std::move(other.trace_))
{
}

}

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
    case LOGGER_ERROR: {
        std::vector<std::string> trace;
        {
            std::lock_guard<std::mutex> guard(buffer_->mutex_);
            if (!buffer_->trace_.empty()) {
                for(auto msg: buffer_->trace_) {
                    if (!msg.empty()) {
                        std::string tmp;
                        std::swap(tmp, msg);
                        trace.push_back(std::move(tmp));
                    }
                }
            }
        }
        if (!trace.empty()) {
            LOG4CXX_TRACE(logger_, "=Begin=trace=======================================================");
            for(auto msg: trace) {
                LOG4CXX_TRACE(logger_, msg);
            }
            LOG4CXX_TRACE(logger_, "==========================================================End=trace=");
        }
        LOG4CXX_ERROR(logger_, str_.str());
        break;
    }
    case BUFFER: {
        std::lock_guard<std::mutex> guard(buffer_->mutex_);
        buffer_->trace_.push_back(str_.str());
        break;
    }
    case NONE:
    break;
    };
}

void Formatter::set_info_sink(log4cxx::LoggerPtr logger) {
    sink_ = LOGGER_INFO;
    logger_ = logger;
}

void Formatter::set_trace_sink(details::CircularBuffer *buffer) {
    sink_ = BUFFER;
    buffer_ = buffer;
}

void Formatter::set_error_sink(log4cxx::LoggerPtr logger, details::CircularBuffer *buffer) {
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

void Logger::init(std::string path) {
    log4cxx::File file(path);
    log4cxx::PropertyConfigurator::configure(file);
}

}  // namespace
