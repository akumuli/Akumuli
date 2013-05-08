/** This is the only header, that can be used by
 *  application code. All interfaces is `extern "C"`
 *  to simplify interopability.
 */

#pragma once
#include <cstdint>
#include "config.h"

extern "C" {

    struct pvalue_t {
        void* address;
        int32_t length;
    };


    // Fwd decl
    struct Database;


    /** Library public interface.
     */
    struct Database {
        // Function signatures
        typedef void (*add_sample_sig) (Database*, int32_t, int32_t, pvalue_t);
        typedef void (*flush_sig) (Database*);
        // Functions
        add_sample_sig add_sample;
        flush_sig flush;
    };


    /** Open existing database.
     *  Function copies config and doesn't stroe any pointers to it.
     */
    Database* open_database(const Config& config);


    /** Close database.
     */
    void close_database(Database* db);
}
