#include "logger.h"

namespace Akumuli {
namespace V2 {

// default logger is console
static aku_logger_cb_t logger_callback = &aku_console_logger;

aku_logger_cb_t Logger::set_logger(aku_logger_cb_t new_logger) {
    auto tmp = logger_callback;
    logger_callback = new_logger;
    return tmp;
}

void Logger::msg(aku_LogLevel lvl, const char* msg) {
    logger_callback(lvl, msg);
}

void Logger::msg(aku_LogLevel lvl, std::string msg) {
    logger_callback(lvl, msg.c_str());
}

}}  // namespaces
