#include "cursor.h"
#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>


namespace Akumuli {

static log4cxx::LoggerPtr s_logger_ = log4cxx::LogManager::getLogger("Akumuli.Cursor");

void RecordingCursor::put(EntryOffset offset) noexcept {
    offsets.push_back(offset);
}

void RecordingCursor::complete() noexcept {
    completed = true;
}

}
