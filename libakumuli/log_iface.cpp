#include "log_iface.h"

namespace Akumuli {

// default logger is console
static aku_logger_cb_t logger_callback = nullptr;

aku_logger_cb_t Logger::set_logger(aku_logger_cb_t new_logger) {
    auto tmp = logger_callback;
    logger_callback = new_logger;
    return tmp;
}

void Logger::msg(aku_LogLevel lvl, const char* msg) {
    if (logger_callback) {
        logger_callback(lvl, msg);
    }
}

void Logger::msg(aku_LogLevel lvl, std::string msg) {
    if (logger_callback) {
        logger_callback(lvl, msg.c_str());
    }
}

}  // namespace
