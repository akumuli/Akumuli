#include "ingestion_pipeline.h"
#include "logger.h"
#include <thread>
#include <iostream>  // TODO: remove iostream

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

// Pipeline spout
PipelineSpout::PipelineSpout(std::shared_ptr<Queue> q)
    : counter_{0}
    , created_(0)
    , deleted_(0)
    , pool_()
    , queue_(q)
{
    pool_.resize(POOL_SIZE);
}

bool r = false;
void PipelineSpout::write_double(aku_ParamId param, aku_TimeStamp ts, double data) {
    int ix = get_index_of_empty_slot();
    if (ix < 0) {
        // Try to delete old items from the pool
        gc();
        ix = get_index_of_empty_slot();
        if (ix < 0) {
            // Impossible to free some space
            // TODO: register data loss
            if (!r) {
                std::cout << "data loss" << std::endl;
                r = true;
            }
            return;
        }
    }
    pool_.at(ix).reset(new TVal{param, ts, data, &counter_});
    auto pvalue = pool_.at(ix).get();
    while (!queue_->push(pvalue)) {
        std::this_thread::yield();
    }
}

void PipelineSpout::add_bulk_string(const Byte *buffer, size_t n) {
    // Shouldn't be implemented
}

int PipelineSpout::get_index_of_empty_slot() {
    if (created_ - deleted_ < POOL_SIZE) {
        // There is some space in the pool
        return created_++;
    }
    return POOL_SIZE + deleted_ - created_;
}

void PipelineSpout::gc() {
    uint64_t processed = counter_;
    while(deleted_ < processed) {
        pool_.at(deleted_ % POOL_SIZE).reset();
        deleted_++;
    }
}

// Ingestion pipeline

IngestionPipeline::IngestionPipeline(std::shared_ptr<DbConnection> con)
    : con_(con)
    , ixmake_{0}
{
    for (int i = N_QUEUES; i --> 0;) {
        queues_.push_back(std::make_shared<PipelineSpout::Queue>());
    }
}

void IngestionPipeline::run() {
    auto qarr = queues_;
    auto con = con_;
    auto worker = [con, qarr]() {
        // Write loop (should be unique)
        PipelineSpout::TVal *val;
        int poison_cnt = 0;
        for (int ix = 0; true; ix++) {
            auto& qref = qarr.at(ix % N_QUEUES);
            for (int i = 0; i < 16; i++) {
                if (qref->pop(val)) {
                    // New write
                    if (val->cnt == nullptr) {  //poisoned
                        poison_cnt++;
                        if (poison_cnt == N_QUEUES) {
                            for (auto& x: qarr) {
                                if (!x->empty()) {
                                    std::cout << "queue not empty" << std::endl;
                                }
                            }
                            return;
                        }
                    }
                    con->write_double(val->id, val->ts, val->value);
                    val->cnt++;
                }
            }
        }
    };

    std::thread th(worker);
    th.detach();
}

std::shared_ptr<PipelineSpout> IngestionPipeline::make_spout() {
    ixmake_++;
    return std::make_shared<PipelineSpout>(queues_.at(ixmake_ % N_QUEUES));
}

PipelineSpout::TVal* IngestionPipeline::POISON = new PipelineSpout::TVal{0, 0, 0, nullptr};

void IngestionPipeline::close() {
    for (auto& q: queues_) {
        q->push(POISON);
    }
}

}
