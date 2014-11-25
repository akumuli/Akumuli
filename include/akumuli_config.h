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
#include <stdint.h>

// Values for durability parameter
#define AKU_MAX_DURABILITY            1  // default value
#define AKU_DURABILITY_SPEED_TRADEOFF 2
#define AKU_MAX_WRITE_SPEED                 4

//! Logging function type
typedef void (*aku_logger_cb_t) (int tag, const char * msg);

//! Panic handler function type
typedef void (*aku_panic_handler_t) (const char * msg);

/** Library configuration.
 */
typedef struct
{
    //! Debug mode trigger
    uint32_t debug_mode;

    //! Pointer to logging function, can be null
    aku_logger_cb_t logger;

    //! 0 - huge tlbs disabled, other value - enabled
    uint32_t enable_huge_tlb;

    //! Consistency-speed tradeoff, 1 - max durability, 2 - tradeoff some durability for speed, 4 - max speed
    uint32_t durability;

} aku_FineTuneParams;

