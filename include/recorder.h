/** This is the only header, that can be used by
 *  application code. All interfaces is `extern "C"`
 *  to simplify interopability.
 */

#pragma once
#include <ctypes>
#include "config.h"

extern "C" {

    typedef struct {
        void* address;
        size_t length;
    } pvalue_t;

    /** Library public interface.
     */
    typedef struct {
        void (*flush) ();
        void (*add_sample) (int32_t param_id, int32_t unix_timestamp, pvalue_t value);
    } Database;

    /** Open existing database.
     *  Function copies config and doesn't stroe any pointers to it.
     */
    Database* open_database(const Config& config);

    /** Close database.
     */
    void close_database(Database* db);
}
