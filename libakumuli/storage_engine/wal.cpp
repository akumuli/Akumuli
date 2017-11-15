#include "wal.h"

namespace Akumuli {
namespace StorageEngine {

AppendOnlyLog::AppendOnlyLog(const char* file_name)
    : file_name_(file_name)
{
}

aku_Status AppendOnlyLog::create_or_open() {
    throw "not implemented";
}

aku_Status AppendOnlyLog::append(const char* block, size_t size) {
    throw "not implemented";
}

}
}
