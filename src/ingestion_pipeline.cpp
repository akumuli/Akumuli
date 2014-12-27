#include "ingestion_pipeline.h"
#include "logger.h"
#include <thread>

namespace Akumuli
{

static Logger logger_ = Logger("IP", 32);

static void db_logger(int tag, const char *msg) {
    logger_.error() << "(" << tag << ") " << msg;
}

AkumuliConnection::AkumuliConnection(const char *path, bool hugetlb, Durability durability)
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

void AkumuliConnection::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    aku_write_double(db_, param, ts, data);
}

// Ingestion pipeline

IngestionPipeline::IngestionPipeline(std::shared_ptr<DbConnection> con)
    : con_(con)
{
}

void IngestionPipeline::start() {
    Queue *qref = &queue_;
    auto con = con_;
    auto worker = [con, qref]() {
        aku_ParamId     id   = 0;
        aku_TimeStamp   ts   = 0;
        double          data = 0;
        bool            pois = false;
        // Write loop (should be unique)
        while(true) {
            const TVal *val;
            if (qref->pop(val)) {
                // New write
                std::tie(id, ts, data, pois) = *val;
                delete val;
                if (pois) {  //poisoned
                    break;
                }
                con->write_double(id, ts, data);
            }
        }
    };

    std::thread th(worker);
    th.detach();
}

void IngestionPipeline::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    const TVal *val = new TVal(param, ts, data, false);
    if (!queue_.push(val)) {
        // TODO: register data loss
        delete val;
    }
}

void IngestionPipeline::add_bulk_string(const Byte *buffer, size_t n) {

}

}
