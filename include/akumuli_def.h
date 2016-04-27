/**
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
 */


#ifndef AKUMULI_DEF_H
#define AKUMULI_DEF_H

#include <stdint.h>

//------------//
// Data types //
//------------//

typedef  uint8_t  u8;
typedef   int8_t  i8;
typedef uint16_t u16;
typedef  int16_t i16;
typedef uint32_t u32;
typedef  int32_t i32;
typedef uint64_t u64;
typedef  int64_t i64;

typedef u64 aku_Timestamp;  //< Timestamp
typedef u64 aku_ParamId;    //< Parameter (or sequence) id

//! Structure represents memory region
typedef struct {
    const void* address;
    u32         length;
} aku_MemRange;


//! Payload data
typedef struct {
    //------------------------------------------//
    //       Normal payload (float value)       //
    //------------------------------------------//

    //! Value
    double float64;

    /** Payload size (payload can be variably sized)
     *  size = 0 means size = sizeof(aku_Sample)
     */
    u16 size;

    //! Data element flags
    enum {
        EMPTY         = 0,
        URGENT        = 1 << 8, /** urgent flag (anomaly or error) */
        PARAMID_BIT   = 1,
        TIMESTAMP_BIT = 1 << 1,
        CUSTOM_TIMESTAMP =
            1 << 2, /** indicates that timestamp shouldn't be formatted during output */
        FLOAT_BIT      = 1 << 4,
        ERROR_CLIPPING = 1 << 9,  /** indicates that some data was lost due to clipping
                                            (extra payload stored in `data` was lost) */
        SAX_WORD       = 1 << 10, /** indicates that SAX word is stored in extra payload */

        MARGIN    = 1 << 13, /** shuld be used to detect margin (if (payload.type > MARGIN) ...) */
        LO_MARGIN = 1 << 14, /** backward direction margin */
        HI_MARGIN = 1 << 15, /** forward direction margin */
    };
    u16 type;

    //---------------------------//
    //       Extra payload       //
    //---------------------------//

    //! Extra payload data
    char data[0];

} aku_PData;

#define AKU_PAYLOAD_FLOAT (aku_PData::PARAMID_BIT | aku_PData::TIMESTAMP_BIT | aku_PData::FLOAT_BIT)

//! Cursor result type
typedef struct {
    aku_Timestamp timestamp;
    aku_ParamId   paramid;
    aku_PData     payload;
} aku_Sample;

// Limits

//! Minimal possible TTL
#define AKU_LIMITS_MIN_TTL 2
#define AKU_LIMITS_MAX_ID 0xFFFFFFFFFFFFFFFDul
//! Max number of tags in series name
#define AKU_LIMITS_MAX_TAGS 32
//! Longest possible series name
#define AKU_LIMITS_MAX_SNAME 0x200
#define AKU_MIN_TIMESTAMP 0ull
#define AKU_MAX_TIMESTAMP (~0ull)
#define AKU_STACK_SIZE 0x100000
#define AKU_HISTOGRAM_SIZE 0x10000
#define AKU_MAX_COLUMNS 8

//! Max number of live generations in cache
#define AKU_LIMITS_MAX_CACHES 8
//! Prepopulation count for cache
#define AKU_CACHE_POPULATION 32

// General error codes

//! Success
typedef enum {
    AKU_SUCCESS = 0,
    //! No data, can't proceed
    AKU_ENO_DATA = 1,
    //! Not enough memory
    AKU_ENO_MEM = 2,
    //! Device is busy
    AKU_EBUSY = 3,
    //! Can't find result
    AKU_ENOT_FOUND = 4,
    //! Bad argument
    AKU_EBAD_ARG = 5,
    //! Overflow error
    AKU_EOVERFLOW = 6,
    //! The suplied data is invalid
    AKU_EBAD_DATA = 7,
    //! Error, no details available
    AKU_EGENERAL = 8,
    //! Late write error
    AKU_ELATE_WRITE = 9,
    //! Not implemented error
    AKU_ENOT_IMPLEMENTED = 10,
    //! Invalid query
    AKU_EQUERY_PARSING_ERROR = 11,
    //! Anomaly detector doesn't supports negative values (now)
    AKU_EANOMALY_NEG_VAL = 12,
    //! Stale data in sequencer, merge to disk required
    AKU_EMERGE_REQUIRED = 13,

    AKU_EMAX_ERROR = 14,
} aku_Status;


// Cursor directions
#define AKU_CURSOR_DIR_FORWARD 0
#define AKU_CURSOR_DIR_BACKWARD 1


// Different tune parameters
#define AKU_INTERPOLATION_SEARCH_CUTOFF 0x00000100

//! If timestamp is greater or less than this value - entry stores compressed chunk
#define AKU_ID_COMPRESSED 0xFFFFFFFFFFFFFFFEul
//! Id for forward scanning
#define AKU_CHUNK_FWD_ID 0xFFFFFFFFFFFFFFFEul
//! Id for backward scanning
#define AKU_CHUNK_BWD_ID 0xFFFFFFFFFFFFFFFFul


//! Default compression threshold - 1000 elements
#define AKU_DEFAULT_COMPRESSION_THRESHOLD 1000

//! Default window with - 1s
#define AKU_DEFAULT_WINDOW_SIZE 1000000000ul

//! Default cache size - 128Mb
#define AKU_DEFAULT_MAX_CACHE_SIZE (1024 * 1024 * 128)

#endif
