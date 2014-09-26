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


// Limits

//! Minimal possible TTL
#define AKU_LIMITS_MIN_TTL        2
#define AKU_LIMITS_MAX_ID         0xFFFFFFFF
#define AKU_MIN_TIMESTAMP         0
#define AKU_MAX_TIMESTAMP       (~0)
#define AKU_STACK_SIZE            0x100000
#define AKU_HISTOGRAM_SIZE        0x10000

//! Max number of live generations in cache
#define AKU_LIMITS_MAX_CACHES     8
//! Prepopulation count for cache
#define AKU_CACHE_POPULATION     32

// General error codes

//! Success
#define AKU_SUCCESS               0
//! No data, can't proceed
#define AKU_ENO_DATA              1
//! Not enough memory
#define AKU_ENO_MEM               2
//! Device is busy
#define AKU_EBUSY                 3
//! Can't find result
#define AKU_ENOT_FOUND            4
//! Bad argument
#define AKU_EBAD_ARG              5
//! Overflow error
#define AKU_EOVERFLOW             6
//! The suplied data is invalid
#define AKU_EBAD_DATA             7
//! Error, no details available
#define AKU_EGENERAL              8
//! Late write error
#define AKU_ELATE_WRITE           9


// Search error codes

//! No error
#define AKU_SEARCH_SUCCESS        0
//! Can't find result
#define AKU_SEARCH_ENOT_FOUND     4
//! Invalid arguments
#define AKU_SEARCH_EBAD_ARG       5

// Config
#define AKU_DEBUG_MODE_ON         1
#define AKU_DEBUG_MODE_OFF        0

// Write status

//! Succesfull write
#define AKU_WRITE_STATUS_SUCCESS  0
//! Page overflow during write
#define AKU_WRITE_STATUS_OVERFLOW 6
//! Invalid input
#define AKU_WRITE_STATUS_BAD_DATA 7


// Cursor directions
#define AKU_CURSOR_DIR_FORWARD    0
#define AKU_CURSOR_DIR_BACKWARD   1


// Different tune parameters
#define AKU_INTERPOLATION_SEARCH_CUTOFF 0x00000100

//! If timestamp is greater or less than this value - entry stores compressed chunk
#define AKU_ID_COMPRESSED               0xFFFFFFFE
//! Id for forward scanning
#define AKU_CHUNK_FWD_ID                0xFFFFFFFE
//! Id for backward scanning
#define AKU_CHUNK_BWD_ID                0xFFFFFFFF

// Defaults
#define AKU_DEFAULT_COMPRESSION_THRESHOLD 0x1000u
#define AKU_DEFAULT_WINDOW_SIZE 10000ul
#define AKU_DEFAULT_MAX_CACHE_SIZE 0x100000u

#endif
