/**
 * PUBLIC HEADER
 *
 * Library configuration data.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#pragma once
#include "akumuli_def.h"

// Values for durability parameter
#define AKU_MAX_DURABILITY 1  // default value
#define AKU_DURABILITY_SPEED_TRADEOFF 2
#define AKU_MAX_WRITE_SPEED 4


// Log levels
typedef enum {
    AKU_LOG_TRACE = 7,
    AKU_LOG_INFO  = 2,
    AKU_LOG_ERROR = 1,
} aku_LogLevel;


//! Logging function type
typedef void (*aku_logger_cb_t)(aku_LogLevel level, const char* msg);


//! Panic handler function type
typedef void (*aku_panic_handler_t)(const char* msg);


/** Library configuration.
 */
typedef struct {
    //! Debug mode trigger
    u32 debug_mode;

    //! Pointer to logging function, can be null
    aku_logger_cb_t logger;

    //! 0 - huge tlbs disabled, other value - enabled
    u32 enable_huge_tlb;

    //! Consistency-speed tradeoff, 1 - max durability, 2 - tradeoff some durability for speed, 4 - max speed
    u32 durability;

    //! Number of data points that should be stored in one compressed chunk
    u32 compression_threshold;

    //! Windth of the sliding window
    u64 window_size;

    //! Cache size limit
    u64 max_cache_size;

} aku_FineTuneParams;
