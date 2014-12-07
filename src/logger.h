#pragma once
#include <sstream>

#include <log4cxx/logger.h>

#include <boost/circular_buffer.hpp>

namespace Akumuli {

class Formatter {
    std::stringstream& str_;
public:
    Formatter(std::stringstream& str);

    template<class T>
    Formatter& operator << (T const& value) {
        str_ << value;
        return *this;
    }
};

/** Logger class. Not thread safe.
  * @code
  * Logger logger("subsystem name");
  * logger.trace() << "Trace message";
  * logger.info() << "Info message";
  * logger.commit(); // info message goes to file
  * logger.error() << "Error message " << e.what(); // trace, info and error goes to file
  */
class Logger
{
    typedef std::unique_ptr<std::string> TraceMessage;
    boost::circular_buffer<TraceMessage> trace_;
    std::stringstream stream_;
    log4cxx::LoggerPtr plogger_;
public:
    Logger(const char* log_name, int depth);

    Formatter& trace();
    Formatter& info();
    Formatter& error();

    void commit();
};

}

#endif // LOGGER_H
