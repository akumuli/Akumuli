#include "ingestion_pipeline.h"
#include "logger.h"

namespace Akumuli
{

static Logger logger_ = Logger("IP", 32);

static void db_logger(int tag, const char *msg) {
    logger_.error() << "(" << tag << ") " << msg;
}

IngestionPipeline::IngestionPipeline(const char *path, bool hugetlb, Durability durability)
    : dbpath_(path)
{
    aku_FineTuneParams params = {
        // Debug mode
        0,
        // Pointer to logging function
        &db_logger,
        // huge tlbs
        (hugetlb ? 1u : 0u),
        // durability
        (uint32_t)durability
    };
    db_ = aku_open_database(dbpath_.c_str(), params);
}

void IngestionPipeline::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    // TODO: move to thread
    aku_write_double(db_, param, ts, data);
}

void IngestionPipeline::add_bulk_string(const Byte *buffer, size_t n) {

}

}
