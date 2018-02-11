/* auto-generated on Sun Feb 11 06:22:46 PST 2018. Do not edit! */
/* begin file /home/dev/Work/lz4/lib/xxhash.h */
/*
   xxHash - Extremely Fast Hash algorithm
   Header File
   Copyright (C) 2012-2016, Yann Collet.

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - xxHash source repository : https://github.com/Cyan4973/xxHash
*/

/* Notice extracted from xxHash homepage :

xxHash is an extremely fast Hash algorithm, running at RAM speed limits.
It also successfully passes all tests from the SMHasher suite.

Comparison (single thread, Windows Seven 32 bits, using SMHasher on a Core 2 Duo @3GHz)

Name            Speed       Q.Score   Author
xxHash          5.4 GB/s     10
CrapWow         3.2 GB/s      2       Andrew
MumurHash 3a    2.7 GB/s     10       Austin Appleby
SpookyHash      2.0 GB/s     10       Bob Jenkins
SBox            1.4 GB/s      9       Bret Mulvey
Lookup3         1.2 GB/s      9       Bob Jenkins
SuperFastHash   1.2 GB/s      1       Paul Hsieh
CityHash64      1.05 GB/s    10       Pike & Alakuijala
FNV             0.55 GB/s     5       Fowler, Noll, Vo
CRC32           0.43 GB/s     9
MD5-32          0.33 GB/s    10       Ronald L. Rivest
SHA1-32         0.28 GB/s    10

Q.Score is a measure of quality of the hash function.
It depends on successfully passing SMHasher test set.
10 is a perfect score.

A 64-bits version, named XXH64, is available since r35.
It offers much better speed, but for 64-bits applications only.
Name     Speed on 64 bits    Speed on 32 bits
XXH64       13.8 GB/s            1.9 GB/s
XXH32        6.8 GB/s            6.0 GB/s
*/

#ifndef XXHASH_H_5627135585666179
#define XXHASH_H_5627135585666179 1

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************
*  Definitions
******************************/
#include <stddef.h>   /* size_t */
typedef enum { XXH_OK=0, XXH_ERROR } XXH_errorcode;


/* ****************************
*  API modifier
******************************/
/** XXH_PRIVATE_API
*   This is useful to include xxhash functions in `static` mode
*   in order to inline them, and remove their symbol from the public list.
*   Methodology :
*     #define XXH_PRIVATE_API
*   `xxhash.c` is automatically included.
*   It's not useful to compile and link it as a separate module.
*/
#define XXH_PRIVATE_API
#ifdef XXH_PRIVATE_API
#  ifndef XXH_STATIC_LINKING_ONLY
#    define XXH_STATIC_LINKING_ONLY
#  endif
#  if defined(__GNUC__)
#    define XXH_PUBLIC_API
#  elif defined (__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#    define XXH_PUBLIC_API static inline
#  elif defined(_MSC_VER)
#    define XXH_PUBLIC_API static __inline
#  else
#    define XXH_PUBLIC_API
#  endif
#else
#  define XXH_PUBLIC_API   /* do nothing */
#endif /* XXH_PRIVATE_API */

/*!XXH_NAMESPACE, aka Namespace Emulation :

If you want to include _and expose_ xxHash functions from within your own library,
but also want to avoid symbol collisions with other libraries which may also include xxHash,

you can use XXH_NAMESPACE, to automatically prefix any public symbol from xxhash library
with the value of XXH_NAMESPACE (therefore, avoid NULL and numeric values).

Note that no change is required within the calling program as long as it includes `xxhash.h` :
regular symbol name will be automatically translated by this header.
*/
#ifdef XXH_NAMESPACE
#  define XXH_CAT(A,B) A##B
#  define XXH_NAME2(A,B) XXH_CAT(A,B)
#  define XXH_versionNumber XXH_NAME2(XXH_NAMESPACE, XXH_versionNumber)
#  define XXH32 XXH_NAME2(XXH_NAMESPACE, XXH32)
#  define XXH32_createState XXH_NAME2(XXH_NAMESPACE, XXH32_createState)
#  define XXH32_freeState XXH_NAME2(XXH_NAMESPACE, XXH32_freeState)
#  define XXH32_reset XXH_NAME2(XXH_NAMESPACE, XXH32_reset)
#  define XXH32_update XXH_NAME2(XXH_NAMESPACE, XXH32_update)
#  define XXH32_digest XXH_NAME2(XXH_NAMESPACE, XXH32_digest)
#  define XXH32_copyState XXH_NAME2(XXH_NAMESPACE, XXH32_copyState)
#  define XXH32_canonicalFromHash XXH_NAME2(XXH_NAMESPACE, XXH32_canonicalFromHash)
#  define XXH32_hashFromCanonical XXH_NAME2(XXH_NAMESPACE, XXH32_hashFromCanonical)
#  define XXH64 XXH_NAME2(XXH_NAMESPACE, XXH64)
#  define XXH64_createState XXH_NAME2(XXH_NAMESPACE, XXH64_createState)
#  define XXH64_freeState XXH_NAME2(XXH_NAMESPACE, XXH64_freeState)
#  define XXH64_reset XXH_NAME2(XXH_NAMESPACE, XXH64_reset)
#  define XXH64_update XXH_NAME2(XXH_NAMESPACE, XXH64_update)
#  define XXH64_digest XXH_NAME2(XXH_NAMESPACE, XXH64_digest)
#  define XXH64_copyState XXH_NAME2(XXH_NAMESPACE, XXH64_copyState)
#  define XXH64_canonicalFromHash XXH_NAME2(XXH_NAMESPACE, XXH64_canonicalFromHash)
#  define XXH64_hashFromCanonical XXH_NAME2(XXH_NAMESPACE, XXH64_hashFromCanonical)
#endif


/* *************************************
*  Version
***************************************/
#define XXH_VERSION_MAJOR    0
#define XXH_VERSION_MINOR    6
#define XXH_VERSION_RELEASE  2
#define XXH_VERSION_NUMBER  (XXH_VERSION_MAJOR *100*100 + XXH_VERSION_MINOR *100 + XXH_VERSION_RELEASE)
XXH_PUBLIC_API unsigned XXH_versionNumber (void);


/*-**********************************************************************
*  32-bits hash
************************************************************************/
typedef unsigned int       XXH32_hash_t;

/*! XXH32() :
    Calculate the 32-bits hash of sequence "length" bytes stored at memory address "input".
    The memory between input & input+length must be valid (allocated and read-accessible).
    "seed" can be used to alter the result predictably.
    Speed on Core 2 Duo @ 3 GHz (single thread, SMHasher benchmark) : 5.4 GB/s */
XXH_PUBLIC_API XXH32_hash_t XXH32 (const void* input, size_t length, unsigned int seed);

/*======   Streaming   ======*/
typedef struct XXH32_state_s XXH32_state_t;   /* incomplete type */
XXH_PUBLIC_API XXH32_state_t* XXH32_createState(void);
XXH_PUBLIC_API XXH_errorcode  XXH32_freeState(XXH32_state_t* statePtr);
XXH_PUBLIC_API void XXH32_copyState(XXH32_state_t* dst_state, const XXH32_state_t* src_state);

XXH_PUBLIC_API XXH_errorcode XXH32_reset  (XXH32_state_t* statePtr, unsigned int seed);
XXH_PUBLIC_API XXH_errorcode XXH32_update (XXH32_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH32_hash_t  XXH32_digest (const XXH32_state_t* statePtr);

/*
These functions generate the xxHash of an input provided in multiple segments.
Note that, for small input, they are slower than single-call functions, due to state management.
For small input, prefer `XXH32()` and `XXH64()` .

XXH state must first be allocated, using XXH*_createState() .

Start a new hash by initializing state with a seed, using XXH*_reset().

Then, feed the hash state by calling XXH*_update() as many times as necessary.
Obviously, input must be allocated and read accessible.
The function returns an error code, with 0 meaning OK, and any other value meaning there is an error.

Finally, a hash value can be produced anytime, by using XXH*_digest().
This function returns the nn-bits hash as an int or long long.

It's still possible to continue inserting input into the hash state after a digest,
and generate some new hashes later on, by calling again XXH*_digest().

When done, free XXH state space if it was allocated dynamically.
*/

/*======   Canonical representation   ======*/

typedef struct { unsigned char digest[4]; } XXH32_canonical_t;
XXH_PUBLIC_API void XXH32_canonicalFromHash(XXH32_canonical_t* dst, XXH32_hash_t hash);
XXH_PUBLIC_API XXH32_hash_t XXH32_hashFromCanonical(const XXH32_canonical_t* src);

/* Default result type for XXH functions are primitive unsigned 32 and 64 bits.
*  The canonical representation uses human-readable write convention, aka big-endian (large digits first).
*  These functions allow transformation of hash result into and from its canonical format.
*  This way, hash values can be written into a file / memory, and remain comparable on different systems and programs.
*/


#ifndef XXH_NO_LONG_LONG
/*-**********************************************************************
*  64-bits hash
************************************************************************/
typedef unsigned long long XXH64_hash_t;

/*! XXH64() :
    Calculate the 64-bits hash of sequence of length "len" stored at memory address "input".
    "seed" can be used to alter the result predictably.
    This function runs faster on 64-bits systems, but slower on 32-bits systems (see benchmark).
*/
XXH_PUBLIC_API XXH64_hash_t XXH64 (const void* input, size_t length, unsigned long long seed);

/*======   Streaming   ======*/
typedef struct XXH64_state_s XXH64_state_t;   /* incomplete type */
XXH_PUBLIC_API XXH64_state_t* XXH64_createState(void);
XXH_PUBLIC_API XXH_errorcode  XXH64_freeState(XXH64_state_t* statePtr);
XXH_PUBLIC_API void XXH64_copyState(XXH64_state_t* dst_state, const XXH64_state_t* src_state);

XXH_PUBLIC_API XXH_errorcode XXH64_reset  (XXH64_state_t* statePtr, unsigned long long seed);
XXH_PUBLIC_API XXH_errorcode XXH64_update (XXH64_state_t* statePtr, const void* input, size_t length);
XXH_PUBLIC_API XXH64_hash_t  XXH64_digest (const XXH64_state_t* statePtr);

/*======   Canonical representation   ======*/
typedef struct { unsigned char digest[8]; } XXH64_canonical_t;
XXH_PUBLIC_API void XXH64_canonicalFromHash(XXH64_canonical_t* dst, XXH64_hash_t hash);
XXH_PUBLIC_API XXH64_hash_t XXH64_hashFromCanonical(const XXH64_canonical_t* src);
#endif  /* XXH_NO_LONG_LONG */


#ifdef XXH_STATIC_LINKING_ONLY

/* ================================================================================================
   This section contains definitions which are not guaranteed to remain stable.
   They may change in future versions, becoming incompatible with a different version of the library.
   They shall only be used with static linking.
   Never use these definitions in association with dynamic linking !
=================================================================================================== */

/* These definitions are only meant to allow allocation of XXH state
   statically, on stack, or in a struct for example.
   Do not use members directly. */

   struct XXH32_state_s {
       unsigned total_len_32;
       unsigned large_len;
       unsigned v1;
       unsigned v2;
       unsigned v3;
       unsigned v4;
       unsigned mem32[4];   /* buffer defined as U32 for alignment */
       unsigned memsize;
       unsigned reserved;   /* never read nor write, will be removed in a future version */
   };   /* typedef'd to XXH32_state_t */

#ifndef XXH_NO_LONG_LONG
   struct XXH64_state_s {
       unsigned long long total_len;
       unsigned long long v1;
       unsigned long long v2;
       unsigned long long v3;
       unsigned long long v4;
       unsigned long long mem64[4];   /* buffer defined as U64 for alignment */
       unsigned memsize;
       unsigned reserved[2];          /* never read nor write, will be removed in a future version */
   };   /* typedef'd to XXH64_state_t */
#endif

#  ifdef XXH_PRIVATE_API
#  endif

#endif /* XXH_STATIC_LINKING_ONLY */


#if defined (__cplusplus)
}
#endif

#endif /* XXHASH_H_5627135585666179 */
/* end file /home/dev/Work/lz4/lib/xxhash.h */
/* begin file /home/dev/Work/lz4/lib/lz4.h */
/*
 *  LZ4 - Fast LZ compression algorithm
 *  Header File
 *  Copyright (C) 2011-2017, Yann Collet.

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
    - LZ4 homepage : http://www.lz4.org
    - LZ4 source repository : https://github.com/lz4/lz4
*/
#if defined (__cplusplus)
extern "C" {
#endif

#ifndef LZ4_H_2983827168210
#define LZ4_H_2983827168210

/* --- Dependency --- */
#include <stddef.h>   /* size_t */


/**
  Introduction

  LZ4 is lossless compression algorithm, providing compression speed at 400 MB/s per core,
  scalable with multi-cores CPU. It features an extremely fast decoder, with speed in
  multiple GB/s per core, typically reaching RAM speed limits on multi-core systems.

  The LZ4 compression library provides in-memory compression and decompression functions.
  Compression can be done in:
    - a single step (described as Simple Functions)
    - a single step, reusing a context (described in Advanced Functions)
    - unbounded multiple steps (described as Streaming compression)

  lz4.h provides block compression functions. It gives full buffer control to user.
  Decompressing an lz4-compressed block also requires metadata (such as compressed size).
  Each application is free to encode such metadata in whichever way it wants.

  An additional format, called LZ4 frame specification (doc/lz4_Frame_format.md),
  take care of encoding standard metadata alongside LZ4-compressed blocks.
  If your application requires interoperability, it's recommended to use it.
  A library is provided to take care of it, see lz4frame.h.
*/

/*^***************************************************************
*  Export parameters
*****************************************************************/
/*
*  LZ4_DLL_EXPORT :
*  Enable exporting of functions when building a Windows DLL
*  LZ4LIB_VISIBILITY :
*  Control library symbols visibility.
*/
#ifndef LZ4LIB_VISIBILITY
#  if defined(__GNUC__) && (__GNUC__ >= 4)
#    define LZ4LIB_VISIBILITY __attribute__ ((visibility ("default")))
#  else
#    define LZ4LIB_VISIBILITY
#  endif
#endif
#if defined(LZ4_DLL_EXPORT) && (LZ4_DLL_EXPORT==1)
#  define LZ4LIB_API __declspec(dllexport) LZ4LIB_VISIBILITY
#elif defined(LZ4_DLL_IMPORT) && (LZ4_DLL_IMPORT==1)
#  define LZ4LIB_API __declspec(dllimport) LZ4LIB_VISIBILITY /* It isn't required but allows to generate better code, saving a function pointer load from the IAT and an indirect jump.*/
#else
#  define LZ4LIB_API LZ4LIB_VISIBILITY
#endif

/*------   Version   ------*/
#define LZ4_VERSION_MAJOR    1    /* for breaking interface changes  */
#define LZ4_VERSION_MINOR    8    /* for new (non-breaking) interface capabilities */
#define LZ4_VERSION_RELEASE  1    /* for tweaks, bug-fixes, or development */

#define LZ4_VERSION_NUMBER (LZ4_VERSION_MAJOR *100*100 + LZ4_VERSION_MINOR *100 + LZ4_VERSION_RELEASE)

#define LZ4_LIB_VERSION LZ4_VERSION_MAJOR.LZ4_VERSION_MINOR.LZ4_VERSION_RELEASE
#define LZ4_QUOTE(str) #str
#define LZ4_EXPAND_AND_QUOTE(str) LZ4_QUOTE(str)
#define LZ4_VERSION_STRING LZ4_EXPAND_AND_QUOTE(LZ4_LIB_VERSION)

LZ4LIB_API int LZ4_versionNumber (void);  /**< library version number; useful to check dll version */
LZ4LIB_API const char* LZ4_versionString (void);   /**< library version string; unseful to check dll version */


/*-************************************
*  Tuning parameter
**************************************/
/*!
 * LZ4_MEMORY_USAGE :
 * Memory usage formula : N->2^N Bytes (examples : 10 -> 1KB; 12 -> 4KB ; 16 -> 64KB; 20 -> 1MB; etc.)
 * Increasing memory usage improves compression ratio
 * Reduced memory usage may improve speed, thanks to cache effect
 * Default value is 14, for 16KB, which nicely fits into Intel x86 L1 cache
 */
#ifndef LZ4_MEMORY_USAGE
# define LZ4_MEMORY_USAGE 14
#endif

/*-************************************
*  Simple Functions
**************************************/
/*! LZ4_compress_default() :
    Compresses 'srcSize' bytes from buffer 'src'
    into already allocated 'dst' buffer of size 'dstCapacity'.
    Compression is guaranteed to succeed if 'dstCapacity' >= LZ4_compressBound(srcSize).
    It also runs faster, so it's a recommended setting.
    If the function cannot compress 'src' into a more limited 'dst' budget,
    compression stops *immediately*, and the function result is zero.
    Note : as a consequence, 'dst' content is not valid.
    Note 2 : This function is protected against buffer overflow scenarios (never writes outside 'dst' buffer, nor read outside 'source' buffer).
        srcSize : max supported value is LZ4_MAX_INPUT_SIZE.
        dstCapacity : size of buffer 'dst' (which must be already allocated)
        return  : the number of bytes written into buffer 'dst' (necessarily <= dstCapacity)
                  or 0 if compression fails */
LZ4LIB_API int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);

/*! LZ4_decompress_safe() :
    compressedSize : is the exact complete size of the compressed block.
    dstCapacity : is the size of destination buffer, which must be already allocated.
    return : the number of bytes decompressed into destination buffer (necessarily <= dstCapacity)
             If destination buffer is not large enough, decoding will stop and output an error code (negative value).
             If the source stream is detected malformed, the function will stop decoding and return a negative result.
             This function is protected against malicious data packets.
*/
LZ4LIB_API int LZ4_decompress_safe (const char* src, char* dst, int compressedSize, int dstCapacity);


/*-************************************
*  Advanced Functions
**************************************/
#define LZ4_MAX_INPUT_SIZE        0x7E000000   /* 2 113 929 216 bytes */
#define LZ4_COMPRESSBOUND(isize)  ((unsigned)(isize) > (unsigned)LZ4_MAX_INPUT_SIZE ? 0 : (isize) + ((isize)/255) + 16)

/*!
LZ4_compressBound() :
    Provides the maximum size that LZ4 compression may output in a "worst case" scenario (input data not compressible)
    This function is primarily useful for memory allocation purposes (destination buffer size).
    Macro LZ4_COMPRESSBOUND() is also provided for compilation-time evaluation (stack memory allocation for example).
    Note that LZ4_compress_default() compresses faster when dstCapacity is >= LZ4_compressBound(srcSize)
        inputSize  : max supported value is LZ4_MAX_INPUT_SIZE
        return : maximum output size in a "worst case" scenario
              or 0, if input size is incorrect (too large or negative)
*/
LZ4LIB_API int LZ4_compressBound(int inputSize);

/*!
LZ4_compress_fast() :
    Same as LZ4_compress_default(), but allows selection of "acceleration" factor.
    The larger the acceleration value, the faster the algorithm, but also the lesser the compression.
    It's a trade-off. It can be fine tuned, with each successive value providing roughly +~3% to speed.
    An acceleration value of "1" is the same as regular LZ4_compress_default()
    Values <= 0 will be replaced by ACCELERATION_DEFAULT (currently == 1, see lz4.c).
*/
LZ4LIB_API int LZ4_compress_fast (const char* src, char* dst, int srcSize, int dstCapacity, int acceleration);


/*!
LZ4_compress_fast_extState() :
    Same compression function, just using an externally allocated memory space to store compression state.
    Use LZ4_sizeofState() to know how much memory must be allocated,
    and allocate it on 8-bytes boundaries (using malloc() typically).
    Then, provide it as 'void* state' to compression function.
*/
LZ4LIB_API int LZ4_sizeofState(void);
LZ4LIB_API int LZ4_compress_fast_extState (void* state, const char* src, char* dst, int srcSize, int dstCapacity, int acceleration);


/*!
LZ4_compress_destSize() :
    Reverse the logic : compresses as much data as possible from 'src' buffer
    into already allocated buffer 'dst' of size 'targetDestSize'.
    This function either compresses the entire 'src' content into 'dst' if it's large enough,
    or fill 'dst' buffer completely with as much data as possible from 'src'.
        *srcSizePtr : will be modified to indicate how many bytes where read from 'src' to fill 'dst'.
                      New value is necessarily <= old value.
        return : Nb bytes written into 'dst' (necessarily <= targetDestSize)
                 or 0 if compression fails
*/
LZ4LIB_API int LZ4_compress_destSize (const char* src, char* dst, int* srcSizePtr, int targetDstSize);


/*!
LZ4_decompress_fast() : **unsafe!**
This function is a bit faster than LZ4_decompress_safe(),
but doesn't provide any security guarantee.
    originalSize : is the uncompressed size to regenerate
                   Destination buffer must be already allocated, and its size must be >= 'originalSize' bytes.
    return : number of bytes read from source buffer (== compressed size).
             If the source stream is detected malformed, the function stops decoding and return a negative result.
    note : This function respects memory boundaries for *properly formed* compressed data.
           However, it does not provide any protection against malicious input.
           It also doesn't know 'src' size, and implies it's >= compressed size.
           Use this function in trusted environment **only**.
*/
LZ4LIB_API int LZ4_decompress_fast (const char* src, char* dst, int originalSize);

/*!
LZ4_decompress_safe_partial() :
    This function decompress a compressed block of size 'srcSize' at position 'src'
    into destination buffer 'dst' of size 'dstCapacity'.
    The function will decompress a minimum of 'targetOutputSize' bytes, and stop after that.
    However, it's not accurate, and may write more than 'targetOutputSize' (but always <= dstCapacity).
   @return : the number of bytes decoded in the destination buffer (necessarily <= dstCapacity)
        Note : this number can also be < targetOutputSize, if compressed block contains less data.
            Therefore, always control how many bytes were decoded.
            If source stream is detected malformed, function returns a negative result.
            This function is protected against malicious data packets.
*/
LZ4LIB_API int LZ4_decompress_safe_partial (const char* src, char* dst, int srcSize, int targetOutputSize, int dstCapacity);


/*-*********************************************
*  Streaming Compression Functions
***********************************************/
typedef union LZ4_stream_u LZ4_stream_t;   /* incomplete type (defined later) */

/*! LZ4_createStream() and LZ4_freeStream() :
 *  LZ4_createStream() will allocate and initialize an `LZ4_stream_t` structure.
 *  LZ4_freeStream() releases its memory.
 */
LZ4LIB_API LZ4_stream_t* LZ4_createStream(void);
LZ4LIB_API int           LZ4_freeStream (LZ4_stream_t* streamPtr);

/*! LZ4_resetStream() :
 *  An LZ4_stream_t structure can be allocated once and re-used multiple times.
 *  Use this function to start compressing a new stream.
 */
LZ4LIB_API void LZ4_resetStream (LZ4_stream_t* streamPtr);

/*! LZ4_loadDict() :
 *  Use this function to load a static dictionary into LZ4_stream_t.
 *  Any previous data will be forgotten, only 'dictionary' will remain in memory.
 *  Loading a size of 0 is allowed, and is the same as reset.
 * @return : dictionary size, in bytes (necessarily <= 64 KB)
 */
LZ4LIB_API int LZ4_loadDict (LZ4_stream_t* streamPtr, const char* dictionary, int dictSize);

/*! LZ4_compress_fast_continue() :
 *  Compress content into 'src' using data from previously compressed blocks, improving compression ratio.
 *  'dst' buffer must be already allocated.
 *  If dstCapacity >= LZ4_compressBound(srcSize), compression is guaranteed to succeed, and runs faster.
 *
 *  Important : The previous 64KB of compressed data is assumed to remain preset and unmodified in memory!
 *              If less than 64KB has been compressed all the data must be present.
 *  Special 1 : If input buffer is a double-buffer, it can have any size, including < 64 KB.
 *  Special 2 : If input buffer is a ring-buffer, it can have any size, including < 64 KB.
 *
 * @return : size of compressed block
 *           or 0 if there is an error (typically, compressed data cannot fit into 'dst')
 *  After an error, the stream status is invalid, it can only be reset or freed.
 */
LZ4LIB_API int LZ4_compress_fast_continue (LZ4_stream_t* streamPtr, const char* src, char* dst, int srcSize, int dstCapacity, int acceleration);

/*! LZ4_saveDict() :
 *  If previously compressed data block is not guaranteed to remain available at its current memory location,
 *  save it into a safer place (char* safeBuffer).
 *  Note : it's not necessary to call LZ4_loadDict() after LZ4_saveDict(), dictionary is immediately usable.
 *  @return : saved dictionary size in bytes (necessarily <= dictSize), or 0 if error.
 */
LZ4LIB_API int LZ4_saveDict (LZ4_stream_t* streamPtr, char* safeBuffer, int dictSize);


/*-**********************************************
*  Streaming Decompression Functions
*  Bufferless synchronous API
************************************************/
typedef union LZ4_streamDecode_u LZ4_streamDecode_t;   /* incomplete type (defined later) */

/*! LZ4_createStreamDecode() and LZ4_freeStreamDecode() :
 *  creation / destruction of streaming decompression tracking structure.
 *  A tracking structure can be re-used multiple times sequentially. */
LZ4LIB_API LZ4_streamDecode_t* LZ4_createStreamDecode(void);
LZ4LIB_API int                 LZ4_freeStreamDecode (LZ4_streamDecode_t* LZ4_stream);

/*! LZ4_setStreamDecode() :
 *  An LZ4_streamDecode_t structure can be allocated once and re-used multiple times.
 *  Use this function to start decompression of a new stream of blocks.
 *  A dictionary can optionnally be set. Use NULL or size 0 for a simple reset order.
 * @return : 1 if OK, 0 if error
 */
LZ4LIB_API int LZ4_setStreamDecode (LZ4_streamDecode_t* LZ4_streamDecode, const char* dictionary, int dictSize);

/*! LZ4_decompress_*_continue() :
 *  These decoding functions allow decompression of consecutive blocks in "streaming" mode.
 *  A block is an unsplittable entity, it must be presented entirely to a decompression function.
 *  Decompression functions only accept one block at a time.
 *  The last 64KB of previously decoded data *must* remain available and unmodified at the memory position where they were decoded.
 *  If less than 64KB of data has been decoded all the data must be present.
 *
 *  Special : if application sets a ring buffer for decompression, it must respect one of the following conditions :
 *  - Exactly same size as encoding buffer, with same update rule (block boundaries at same positions)
 *    In which case, the decoding & encoding ring buffer can have any size, including very small ones ( < 64 KB).
 *  - Larger than encoding buffer, by a minimum of maxBlockSize more bytes.
 *    maxBlockSize is implementation dependent. It's the maximum size of any single block.
 *    In which case, encoding and decoding buffers do not need to be synchronized,
 *    and encoding ring buffer can have any size, including small ones ( < 64 KB).
 *  - _At least_ 64 KB + 8 bytes + maxBlockSize.
 *    In which case, encoding and decoding buffers do not need to be synchronized,
 *    and encoding ring buffer can have any size, including larger than decoding buffer.
 *  Whenever these conditions are not possible, save the last 64KB of decoded data into a safe buffer,
 *  and indicate where it is saved using LZ4_setStreamDecode() before decompressing next block.
*/
LZ4LIB_API int LZ4_decompress_safe_continue (LZ4_streamDecode_t* LZ4_streamDecode, const char* src, char* dst, int srcSize, int dstCapacity);
LZ4LIB_API int LZ4_decompress_fast_continue (LZ4_streamDecode_t* LZ4_streamDecode, const char* src, char* dst, int originalSize);


/*! LZ4_decompress_*_usingDict() :
 *  These decoding functions work the same as
 *  a combination of LZ4_setStreamDecode() followed by LZ4_decompress_*_continue()
 *  They are stand-alone, and don't need an LZ4_streamDecode_t structure.
 */
LZ4LIB_API int LZ4_decompress_safe_usingDict (const char* src, char* dst, int srcSize, int dstCapcity, const char* dictStart, int dictSize);
LZ4LIB_API int LZ4_decompress_fast_usingDict (const char* src, char* dst, int originalSize, const char* dictStart, int dictSize);


/*^**********************************************
 * !!!!!!   STATIC LINKING ONLY   !!!!!!
 ***********************************************/
/*-************************************
 *  Private definitions
 **************************************
 * Do not use these definitions.
 * They are exposed to allow static allocation of `LZ4_stream_t` and `LZ4_streamDecode_t`.
 * Using these definitions will expose code to API and/or ABI break in future versions of the library.
 **************************************/
#define LZ4_HASHLOG   (LZ4_MEMORY_USAGE-2)
#define LZ4_HASHTABLESIZE (1 << LZ4_MEMORY_USAGE)
#define LZ4_HASH_SIZE_U32 (1 << LZ4_HASHLOG)       /* required as macro for static allocation */

#if defined(__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#include <stdint.h>

typedef struct {
    uint32_t hashTable[LZ4_HASH_SIZE_U32];
    uint32_t currentOffset;
    uint32_t initCheck;
    const uint8_t* dictionary;
    uint8_t* bufferStart;   /* obsolete, used for slideInputBuffer */
    uint32_t dictSize;
} LZ4_stream_t_internal;

typedef struct {
    const uint8_t* externalDict;
    size_t extDictSize;
    const uint8_t* prefixEnd;
    size_t prefixSize;
} LZ4_streamDecode_t_internal;

#else

typedef struct {
    unsigned int hashTable[LZ4_HASH_SIZE_U32];
    unsigned int currentOffset;
    unsigned int initCheck;
    const unsigned char* dictionary;
    unsigned char* bufferStart;   /* obsolete, used for slideInputBuffer */
    unsigned int dictSize;
} LZ4_stream_t_internal;

typedef struct {
    const unsigned char* externalDict;
    size_t extDictSize;
    const unsigned char* prefixEnd;
    size_t prefixSize;
} LZ4_streamDecode_t_internal;

#endif

/*!
 * LZ4_stream_t :
 * information structure to track an LZ4 stream.
 * init this structure before first use.
 * note : only use in association with static linking !
 *        this definition is not API/ABI safe,
 *        it may change in a future version !
 */
#define LZ4_STREAMSIZE_U64 ((1 << (LZ4_MEMORY_USAGE-3)) + 4)
#define LZ4_STREAMSIZE     (LZ4_STREAMSIZE_U64 * sizeof(unsigned long long))
union LZ4_stream_u {
    unsigned long long table[LZ4_STREAMSIZE_U64];
    LZ4_stream_t_internal internal_donotuse;
} ;  /* previously typedef'd to LZ4_stream_t */


/*!
 * LZ4_streamDecode_t :
 * information structure to track an LZ4 stream during decompression.
 * init this structure  using LZ4_setStreamDecode (or memset()) before first use
 * note : only use in association with static linking !
 *        this definition is not API/ABI safe,
 *        and may change in a future version !
 */
#define LZ4_STREAMDECODESIZE_U64  4
#define LZ4_STREAMDECODESIZE     (LZ4_STREAMDECODESIZE_U64 * sizeof(unsigned long long))
union LZ4_streamDecode_u {
    unsigned long long table[LZ4_STREAMDECODESIZE_U64];
    LZ4_streamDecode_t_internal internal_donotuse;
} ;   /* previously typedef'd to LZ4_streamDecode_t */


/*-************************************
*  Obsolete Functions
**************************************/

/*! Deprecation warnings
   Should deprecation warnings be a problem,
   it is generally possible to disable them,
   typically with -Wno-deprecated-declarations for gcc
   or _CRT_SECURE_NO_WARNINGS in Visual.
   Otherwise, it's also possible to define LZ4_DISABLE_DEPRECATE_WARNINGS */
#ifdef LZ4_DISABLE_DEPRECATE_WARNINGS
#  define LZ4_DEPRECATED(message)   /* disable deprecation warnings */
#else
#  define LZ4_GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#  if defined (__cplusplus) && (__cplusplus >= 201402) /* C++14 or greater */
#    define LZ4_DEPRECATED(message) [[deprecated(message)]]
#  elif (LZ4_GCC_VERSION >= 405) || defined(__clang__)
#    define LZ4_DEPRECATED(message) __attribute__((deprecated(message)))
#  elif (LZ4_GCC_VERSION >= 301)
#    define LZ4_DEPRECATED(message) __attribute__((deprecated))
#  elif defined(_MSC_VER)
#    define LZ4_DEPRECATED(message) __declspec(deprecated(message))
#  else
#    pragma message("WARNING: You need to implement LZ4_DEPRECATED for this compiler")
#    define LZ4_DEPRECATED(message)
#  endif
#endif /* LZ4_DISABLE_DEPRECATE_WARNINGS */

/* Obsolete compression functions */
LZ4_DEPRECATED("use LZ4_compress_default() instead") LZ4LIB_API int LZ4_compress               (const char* source, char* dest, int sourceSize);
LZ4_DEPRECATED("use LZ4_compress_default() instead") LZ4LIB_API int LZ4_compress_limitedOutput (const char* source, char* dest, int sourceSize, int maxOutputSize);
LZ4_DEPRECATED("use LZ4_compress_fast_extState() instead") LZ4LIB_API int LZ4_compress_withState               (void* state, const char* source, char* dest, int inputSize);
LZ4_DEPRECATED("use LZ4_compress_fast_extState() instead") LZ4LIB_API int LZ4_compress_limitedOutput_withState (void* state, const char* source, char* dest, int inputSize, int maxOutputSize);
LZ4_DEPRECATED("use LZ4_compress_fast_continue() instead") LZ4LIB_API int LZ4_compress_continue                (LZ4_stream_t* LZ4_streamPtr, const char* source, char* dest, int inputSize);
LZ4_DEPRECATED("use LZ4_compress_fast_continue() instead") LZ4LIB_API int LZ4_compress_limitedOutput_continue  (LZ4_stream_t* LZ4_streamPtr, const char* source, char* dest, int inputSize, int maxOutputSize);

/* Obsolete decompression functions */
LZ4_DEPRECATED("use LZ4_decompress_fast() instead") LZ4LIB_API int LZ4_uncompress (const char* source, char* dest, int outputSize);
LZ4_DEPRECATED("use LZ4_decompress_safe() instead") LZ4LIB_API int LZ4_uncompress_unknownOutputSize (const char* source, char* dest, int isize, int maxOutputSize);

/* Obsolete streaming functions; use new streaming interface whenever possible */
LZ4_DEPRECATED("use LZ4_createStream() instead") LZ4LIB_API void* LZ4_create (char* inputBuffer);
LZ4_DEPRECATED("use LZ4_createStream() instead") LZ4LIB_API int   LZ4_sizeofStreamState(void);
LZ4_DEPRECATED("use LZ4_resetStream() instead") LZ4LIB_API  int   LZ4_resetStreamState(void* state, char* inputBuffer);
LZ4_DEPRECATED("use LZ4_saveDict() instead") LZ4LIB_API     char* LZ4_slideInputBuffer (void* state);

/* Obsolete streaming decoding functions */
LZ4_DEPRECATED("use LZ4_decompress_safe_usingDict() instead") LZ4LIB_API int LZ4_decompress_safe_withPrefix64k (const char* src, char* dst, int compressedSize, int maxDstSize);
LZ4_DEPRECATED("use LZ4_decompress_fast_usingDict() instead") LZ4LIB_API int LZ4_decompress_fast_withPrefix64k (const char* src, char* dst, int originalSize);

#endif /* LZ4_H_2983827168210 */


#if defined (__cplusplus)
}
#endif
/* end file /home/dev/Work/lz4/lib/lz4.h */
/* begin file /home/dev/Work/lz4/lib/lz4hc.h */
/*
   LZ4 HC - High Compression Mode of LZ4
   Header File
   Copyright (C) 2011-2017, Yann Collet.
   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - LZ4 source repository : https://github.com/lz4/lz4
   - LZ4 public forum : https://groups.google.com/forum/#!forum/lz4c
*/
#ifndef LZ4_HC_H_19834876238432
#define LZ4_HC_H_19834876238432

#if defined (__cplusplus)
extern "C" {
#endif

/* --- Dependency --- */
/* note : lz4hc requires lz4.h/lz4.c for compilation */


/* --- Useful constants --- */
#define LZ4HC_CLEVEL_MIN         3
#define LZ4HC_CLEVEL_DEFAULT     9
#define LZ4HC_CLEVEL_OPT_MIN    10
#define LZ4HC_CLEVEL_MAX        12


/*-************************************
 *  Block Compression
 **************************************/
/*! LZ4_compress_HC() :
 *  Compress data from `src` into `dst`, using the more powerful but slower "HC" algorithm.
 * `dst` must be already allocated.
 *  Compression is guaranteed to succeed if `dstCapacity >= LZ4_compressBound(srcSize)` (see "lz4.h")
 *  Max supported `srcSize` value is LZ4_MAX_INPUT_SIZE (see "lz4.h")
 * `compressionLevel` : any value between 1 and LZ4HC_CLEVEL_MAX will work.
 *                      Values > LZ4HC_CLEVEL_MAX behave the same as LZ4HC_CLEVEL_MAX.
 * @return : the number of bytes written into 'dst'
 *           or 0 if compression fails.
 */
LZ4LIB_API int LZ4_compress_HC (const char* src, char* dst, int srcSize, int dstCapacity, int compressionLevel);


/* Note :
 *   Decompression functions are provided within "lz4.h" (BSD license)
 */


/*! LZ4_compress_HC_extStateHC() :
 *  Same as LZ4_compress_HC(), but using an externally allocated memory segment for `state`.
 * `state` size is provided by LZ4_sizeofStateHC().
 *  Memory segment must be aligned on 8-bytes boundaries (which a normal malloc() should do properly).
 */
LZ4LIB_API int LZ4_sizeofStateHC(void);
LZ4LIB_API int LZ4_compress_HC_extStateHC(void* state, const char* src, char* dst, int srcSize, int maxDstSize, int compressionLevel);


/*-************************************
 *  Streaming Compression
 *  Bufferless synchronous API
 **************************************/
 typedef union LZ4_streamHC_u LZ4_streamHC_t;   /* incomplete type (defined later) */

/*! LZ4_createStreamHC() and LZ4_freeStreamHC() :
 *  These functions create and release memory for LZ4 HC streaming state.
 *  Newly created states are automatically initialized.
 *  Existing states can be re-used several times, using LZ4_resetStreamHC().
 *  These methods are API and ABI stable, they can be used in combination with a DLL.
 */
LZ4LIB_API LZ4_streamHC_t* LZ4_createStreamHC(void);
LZ4LIB_API int             LZ4_freeStreamHC (LZ4_streamHC_t* streamHCPtr);

LZ4LIB_API void LZ4_resetStreamHC (LZ4_streamHC_t* streamHCPtr, int compressionLevel);
LZ4LIB_API int  LZ4_loadDictHC (LZ4_streamHC_t* streamHCPtr, const char* dictionary, int dictSize);

LZ4LIB_API int LZ4_compress_HC_continue (LZ4_streamHC_t* streamHCPtr, const char* src, char* dst, int srcSize, int maxDstSize);

LZ4LIB_API int LZ4_saveDictHC (LZ4_streamHC_t* streamHCPtr, char* safeBuffer, int maxDictSize);

/*
  These functions compress data in successive blocks of any size, using previous blocks as dictionary.
  One key assumption is that previous blocks (up to 64 KB) remain read-accessible while compressing next blocks.
  There is an exception for ring buffers, which can be smaller than 64 KB.
  Ring buffers scenario is automatically detected and handled by LZ4_compress_HC_continue().

  Before starting compression, state must be properly initialized, using LZ4_resetStreamHC().
  A first "fictional block" can then be designated as initial dictionary, using LZ4_loadDictHC() (Optional).

  Then, use LZ4_compress_HC_continue() to compress each successive block.
  Previous memory blocks (including initial dictionary when present) must remain accessible and unmodified during compression.
  'dst' buffer should be sized to handle worst case scenarios (see LZ4_compressBound()), to ensure operation success.
  Because in case of failure, the API does not guarantee context recovery, and context will have to be reset.
  If `dst` buffer budget cannot be >= LZ4_compressBound(), consider using LZ4_compress_HC_continue_destSize() instead.

  If, for any reason, previous data block can't be preserved unmodified in memory for next compression block,
  you can save it to a more stable memory space, using LZ4_saveDictHC().
  Return value of LZ4_saveDictHC() is the size of dictionary effectively saved into 'safeBuffer'.
*/


/*-**************************************************************
 * PRIVATE DEFINITIONS :
 * Do not use these definitions.
 * They are exposed to allow static allocation of `LZ4_streamHC_t`.
 * Using these definitions makes the code vulnerable to potential API break when upgrading LZ4
 ****************************************************************/
#define LZ4HC_DICTIONARY_LOGSIZE 16
#define LZ4HC_MAXD (1<<LZ4HC_DICTIONARY_LOGSIZE)
#define LZ4HC_MAXD_MASK (LZ4HC_MAXD - 1)

#define LZ4HC_HASH_LOG 15
#define LZ4HC_HASHTABLESIZE (1 << LZ4HC_HASH_LOG)
#define LZ4HC_HASH_MASK (LZ4HC_HASHTABLESIZE - 1)


#if defined(__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#include <stdint.h>

typedef struct
{
    uint32_t   hashTable[LZ4HC_HASHTABLESIZE];
    uint16_t   chainTable[LZ4HC_MAXD];
    const uint8_t* end;         /* next block here to continue on current prefix */
    const uint8_t* base;        /* All index relative to this position */
    const uint8_t* dictBase;    /* alternate base for extDict */
    uint8_t* inputBuffer;       /* deprecated */
    uint32_t   dictLimit;       /* below that point, need extDict */
    uint32_t   lowLimit;        /* below that point, no more dict */
    uint32_t   nextToUpdate;    /* index from which to continue dictionary update */
    int        compressionLevel;
} LZ4HC_CCtx_internal;

#else

typedef struct
{
    unsigned int   hashTable[LZ4HC_HASHTABLESIZE];
    unsigned short chainTable[LZ4HC_MAXD];
    const unsigned char* end;        /* next block here to continue on current prefix */
    const unsigned char* base;       /* All index relative to this position */
    const unsigned char* dictBase;   /* alternate base for extDict */
    unsigned char* inputBuffer;      /* deprecated */
    unsigned int   dictLimit;        /* below that point, need extDict */
    unsigned int   lowLimit;         /* below that point, no more dict */
    unsigned int   nextToUpdate;     /* index from which to continue dictionary update */
    int            compressionLevel;
} LZ4HC_CCtx_internal;

#endif

#define LZ4_STREAMHCSIZE       (4*LZ4HC_HASHTABLESIZE + 2*LZ4HC_MAXD + 56) /* 262200 */
#define LZ4_STREAMHCSIZE_SIZET (LZ4_STREAMHCSIZE / sizeof(size_t))
union LZ4_streamHC_u {
    size_t table[LZ4_STREAMHCSIZE_SIZET];
    LZ4HC_CCtx_internal internal_donotuse;
};   /* previously typedef'd to LZ4_streamHC_t */
/*
  LZ4_streamHC_t :
  This structure allows static allocation of LZ4 HC streaming state.
  State must be initialized using LZ4_resetStreamHC() before first use.

  Static allocation shall only be used in combination with static linking.
  When invoking LZ4 from a DLL, use create/free functions instead, which are API and ABI stable.
*/


/*-************************************
*  Deprecated Functions
**************************************/
/* see lz4.h LZ4_DISABLE_DEPRECATE_WARNINGS to turn off deprecation warnings */

/* deprecated compression functions */
LZ4_DEPRECATED("use LZ4_compress_HC() instead") LZ4LIB_API int LZ4_compressHC               (const char* source, char* dest, int inputSize);
LZ4_DEPRECATED("use LZ4_compress_HC() instead") LZ4LIB_API int LZ4_compressHC_limitedOutput (const char* source, char* dest, int inputSize, int maxOutputSize);
LZ4_DEPRECATED("use LZ4_compress_HC() instead") LZ4LIB_API int LZ4_compressHC2 (const char* source, char* dest, int inputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_compress_HC() instead") LZ4LIB_API int LZ4_compressHC2_limitedOutput (const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_compress_HC_extStateHC() instead") LZ4LIB_API int LZ4_compressHC_withStateHC               (void* state, const char* source, char* dest, int inputSize);
LZ4_DEPRECATED("use LZ4_compress_HC_extStateHC() instead") LZ4LIB_API int LZ4_compressHC_limitedOutput_withStateHC (void* state, const char* source, char* dest, int inputSize, int maxOutputSize);
LZ4_DEPRECATED("use LZ4_compress_HC_extStateHC() instead") LZ4LIB_API int LZ4_compressHC2_withStateHC (void* state, const char* source, char* dest, int inputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_compress_HC_extStateHC() instead") LZ4LIB_API int LZ4_compressHC2_limitedOutput_withStateHC(void* state, const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_compress_HC_continue() instead") LZ4LIB_API int LZ4_compressHC_continue               (LZ4_streamHC_t* LZ4_streamHCPtr, const char* source, char* dest, int inputSize);
LZ4_DEPRECATED("use LZ4_compress_HC_continue() instead") LZ4LIB_API int LZ4_compressHC_limitedOutput_continue (LZ4_streamHC_t* LZ4_streamHCPtr, const char* source, char* dest, int inputSize, int maxOutputSize);

/* Deprecated Streaming functions using older model; should no longer be used */
LZ4_DEPRECATED("use LZ4_createStreamHC() instead") LZ4LIB_API void* LZ4_createHC (char* inputBuffer);
LZ4_DEPRECATED("use LZ4_saveDictHC() instead") LZ4LIB_API     char* LZ4_slideInputBufferHC (void* LZ4HC_Data);
LZ4_DEPRECATED("use LZ4_freeStreamHC() instead") LZ4LIB_API   int   LZ4_freeHC (void* LZ4HC_Data);
LZ4_DEPRECATED("use LZ4_compress_HC_continue() instead") LZ4LIB_API int LZ4_compressHC2_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_compress_HC_continue() instead") LZ4LIB_API int LZ4_compressHC2_limitedOutput_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel);
LZ4_DEPRECATED("use LZ4_createStreamHC() instead") LZ4LIB_API int   LZ4_sizeofStreamStateHC(void);
LZ4_DEPRECATED("use LZ4_resetStreamHC() instead") LZ4LIB_API  int   LZ4_resetStreamStateHC(void* state, char* inputBuffer);


#if defined (__cplusplus)
}
#endif

#endif /* LZ4_HC_H_19834876238432 */


/*-**************************************************
 * !!!!!     STATIC LINKING ONLY     !!!!!
 * Following definitions are considered experimental.
 * They should not be linked from DLL,
 * as there is no guarantee of API stability yet.
 * Prototypes will be promoted to "stable" status
 * after successfull usage in real-life scenarios.
 ***************************************************/
#ifdef LZ4_HC_STATIC_LINKING_ONLY   /* protection macro */
#ifndef LZ4_HC_SLO_098092834
#define LZ4_HC_SLO_098092834

/*! LZ4_compress_HC_destSize() : v1.8.0 (experimental)
 *  Will try to compress as much data from `src` as possible
 *  that can fit into `targetDstSize` budget.
 *  Result is provided in 2 parts :
 * @return : the number of bytes written into 'dst'
 *           or 0 if compression fails.
 * `srcSizePtr` : value will be updated to indicate how much bytes were read from `src`
 */
int LZ4_compress_HC_destSize(void* LZ4HC_Data,
                            const char* src, char* dst,
                            int* srcSizePtr, int targetDstSize,
                            int compressionLevel);

/*! LZ4_compress_HC_continue_destSize() : v1.8.0 (experimental)
 *  Similar as LZ4_compress_HC_continue(),
 *  but will read a variable nb of bytes from `src`
 *  to fit into `targetDstSize` budget.
 *  Result is provided in 2 parts :
 * @return : the number of bytes written into 'dst'
 *           or 0 if compression fails.
 * `srcSizePtr` : value will be updated to indicate how much bytes were read from `src`.
 */
int LZ4_compress_HC_continue_destSize(LZ4_streamHC_t* LZ4_streamHCPtr,
                            const char* src, char* dst,
                            int* srcSizePtr, int targetDstSize);

/*! LZ4_setCompressionLevel() : v1.8.0 (experimental)
 *  It's possible to change compression level between 2 invocations of LZ4_compress_HC_continue*()
 */
void LZ4_setCompressionLevel(LZ4_streamHC_t* LZ4_streamHCPtr, int compressionLevel);



#endif   /* LZ4_HC_SLO_098092834 */
#endif   /* LZ4_HC_STATIC_LINKING_ONLY */
/* end file /home/dev/Work/lz4/lib/lz4hc.h */
/* begin file /home/dev/Work/lz4/lib/lz4frame.h */
/*
   LZ4 auto-framing library
   Header File
   Copyright (C) 2011-2017, Yann Collet.
   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - LZ4 source repository : https://github.com/lz4/lz4
   - LZ4 public forum : https://groups.google.com/forum/#!forum/lz4c
*/

/* LZ4F is a stand-alone API to create LZ4-compressed frames
 * conformant with specification v1.5.1.
 * It also offers streaming capabilities.
 * lz4.h is not required when using lz4frame.h.
 * */

#ifndef LZ4F_H_09782039843
#define LZ4F_H_09782039843

#if defined (__cplusplus)
extern "C" {
#endif

/* ---   Dependency   --- */
#include <stddef.h>   /* size_t */


/**
  Introduction

  lz4frame.h implements LZ4 frame specification (doc/lz4_Frame_format.md).
  lz4frame.h provides frame compression functions that take care
  of encoding standard metadata alongside LZ4-compressed blocks.
*/

/*-***************************************************************
 *  Compiler specifics
 *****************************************************************/
/*  LZ4_DLL_EXPORT :
 *  Enable exporting of functions when building a Windows DLL
 *  LZ4FLIB_API :
 *  Control library symbols visibility.
 */
#if defined(LZ4_DLL_EXPORT) && (LZ4_DLL_EXPORT==1)
#  define LZ4FLIB_API __declspec(dllexport)
#elif defined(LZ4_DLL_IMPORT) && (LZ4_DLL_IMPORT==1)
#  define LZ4FLIB_API __declspec(dllimport)
#elif defined(__GNUC__) && (__GNUC__ >= 4)
#  define LZ4FLIB_API __attribute__ ((__visibility__ ("default")))
#else
#  define LZ4FLIB_API
#endif

#ifdef LZ4F_DISABLE_DEPRECATE_WARNINGS
#  define LZ4F_DEPRECATE(x) x
#else
#  if defined(_MSC_VER)
#    define LZ4F_DEPRECATE(x) x   /* __declspec(deprecated) x - only works with C++ */
#  elif defined(__clang__) || (defined(__GNUC__) && (__GNUC__ >= 6))
#    define LZ4F_DEPRECATE(x) x __attribute__((deprecated))
#  else
#    define LZ4F_DEPRECATE(x) x   /* no deprecation warning for this compiler */
#  endif
#endif


/*-************************************
 *  Error management
 **************************************/
typedef size_t LZ4F_errorCode_t;

LZ4FLIB_API unsigned    LZ4F_isError(LZ4F_errorCode_t code);   /**< tells if a `LZ4F_errorCode_t` function result is an error code */
LZ4FLIB_API const char* LZ4F_getErrorName(LZ4F_errorCode_t code);   /**< return error code string; useful for debugging */


/*-************************************
 *  Frame compression types
 **************************************/
/* #define LZ4F_ENABLE_OBSOLETE_ENUMS   // uncomment to enable obsolete enums */
#ifdef LZ4F_ENABLE_OBSOLETE_ENUMS
#  define LZ4F_OBSOLETE_ENUM(x) , LZ4F_DEPRECATE(x) = LZ4F_##x
#else
#  define LZ4F_OBSOLETE_ENUM(x)
#endif

/* The larger the block size, the (slightly) better the compression ratio,
 * though there are diminishing returns.
 * Larger blocks also increase memory usage on both compression and decompression sides. */
typedef enum {
    LZ4F_default=0,
    LZ4F_max64KB=4,
    LZ4F_max256KB=5,
    LZ4F_max1MB=6,
    LZ4F_max4MB=7
    LZ4F_OBSOLETE_ENUM(max64KB)
    LZ4F_OBSOLETE_ENUM(max256KB)
    LZ4F_OBSOLETE_ENUM(max1MB)
    LZ4F_OBSOLETE_ENUM(max4MB)
} LZ4F_blockSizeID_t;

/* Linked blocks sharply reduce inefficiencies when using small blocks,
 * they compress better.
 * However, some LZ4 decoders are only compatible with independent blocks */
typedef enum {
    LZ4F_blockLinked=0,
    LZ4F_blockIndependent
    LZ4F_OBSOLETE_ENUM(blockLinked)
    LZ4F_OBSOLETE_ENUM(blockIndependent)
} LZ4F_blockMode_t;

typedef enum {
    LZ4F_noContentChecksum=0,
    LZ4F_contentChecksumEnabled
    LZ4F_OBSOLETE_ENUM(noContentChecksum)
    LZ4F_OBSOLETE_ENUM(contentChecksumEnabled)
} LZ4F_contentChecksum_t;

typedef enum {
    LZ4F_noBlockChecksum=0,
    LZ4F_blockChecksumEnabled
} LZ4F_blockChecksum_t;

typedef enum {
    LZ4F_frame=0,
    LZ4F_skippableFrame
    LZ4F_OBSOLETE_ENUM(skippableFrame)
} LZ4F_frameType_t;

#ifdef LZ4F_ENABLE_OBSOLETE_ENUMS
typedef LZ4F_blockSizeID_t blockSizeID_t;
typedef LZ4F_blockMode_t blockMode_t;
typedef LZ4F_frameType_t frameType_t;
typedef LZ4F_contentChecksum_t contentChecksum_t;
#endif

/*! LZ4F_frameInfo_t :
 *  makes it possible to set or read frame parameters.
 *  It's not required to set all fields, as long as the structure was initially memset() to zero.
 *  For all fields, 0 sets it to default value */
typedef struct {
  LZ4F_blockSizeID_t     blockSizeID;          /* max64KB, max256KB, max1MB, max4MB ; 0 == default */
  LZ4F_blockMode_t       blockMode;            /* LZ4F_blockLinked, LZ4F_blockIndependent ; 0 == default */
  LZ4F_contentChecksum_t contentChecksumFlag;  /* if enabled, frame is terminated with a 32-bits checksum of decompressed data ; 0 == disabled (default)  */
  LZ4F_frameType_t       frameType;            /* read-only field : LZ4F_frame or LZ4F_skippableFrame */
  unsigned long long     contentSize;          /* Size of uncompressed content ; 0 == unknown */
  unsigned               dictID;               /* Dictionary ID, sent by the compressor to help decoder select the correct dictionary; 0 == no dictID provided */
  LZ4F_blockChecksum_t   blockChecksumFlag;    /* if enabled, each block is followed by a checksum of block's compressed data ; 0 == disabled (default)  */
} LZ4F_frameInfo_t;

/*! LZ4F_preferences_t :
 *  makes it possible to supply detailed compression parameters to the stream interface.
 *  It's not required to set all fields, as long as the structure was initially memset() to zero.
 *  All reserved fields must be set to zero. */
typedef struct {
  LZ4F_frameInfo_t frameInfo;
  int      compressionLevel;       /* 0 == default (fast mode); values above LZ4HC_CLEVEL_MAX count as LZ4HC_CLEVEL_MAX; values below 0 trigger "fast acceleration", proportional to value */
  unsigned autoFlush;              /* 1 == always flush, to reduce usage of internal buffers */
  unsigned reserved[4];            /* must be zero for forward compatibility */
} LZ4F_preferences_t;

LZ4FLIB_API int LZ4F_compressionLevel_max(void);


/*-*********************************
*  Simple compression function
***********************************/
/*! LZ4F_compressFrameBound() :
 *  Returns the maximum possible compressed size with LZ4F_compressFrame() given srcSize and preferences.
 * `preferencesPtr` is optional. It can be replaced by NULL, in which case, the function will assume default preferences.
 *  Note : this result is only usable with LZ4F_compressFrame().
 *         It may also be used with LZ4F_compressUpdate() _if no flush() operation_ is performed.
 */
LZ4FLIB_API size_t LZ4F_compressFrameBound(size_t srcSize, const LZ4F_preferences_t* preferencesPtr);

/*! LZ4F_compressFrame() :
 *  Compress an entire srcBuffer into a valid LZ4 frame.
 *  dstCapacity MUST be >= LZ4F_compressFrameBound(srcSize, preferencesPtr).
 *  The LZ4F_preferences_t structure is optional : you can provide NULL as argument. All preferences will be set to default.
 * @return : number of bytes written into dstBuffer.
 *           or an error code if it fails (can be tested using LZ4F_isError())
 */
LZ4FLIB_API size_t LZ4F_compressFrame(void* dstBuffer, size_t dstCapacity,
                                const void* srcBuffer, size_t srcSize,
                                const LZ4F_preferences_t* preferencesPtr);


/*-***********************************
*  Advanced compression functions
*************************************/
typedef struct LZ4F_cctx_s LZ4F_cctx;   /* incomplete type */
typedef LZ4F_cctx* LZ4F_compressionContext_t;   /* for compatibility with previous API version */

typedef struct {
  unsigned stableSrc;    /* 1 == src content will remain present on future calls to LZ4F_compress(); skip copying src content within tmp buffer */
  unsigned reserved[3];
} LZ4F_compressOptions_t;

/*---   Resource Management   ---*/

#define LZ4F_VERSION 100
LZ4FLIB_API unsigned LZ4F_getVersion(void);
/*! LZ4F_createCompressionContext() :
 * The first thing to do is to create a compressionContext object, which will be used in all compression operations.
 * This is achieved using LZ4F_createCompressionContext(), which takes as argument a version.
 * The version provided MUST be LZ4F_VERSION. It is intended to track potential version mismatch, notably when using DLL.
 * The function will provide a pointer to a fully allocated LZ4F_cctx object.
 * If @return != zero, there was an error during context creation.
 * Object can release its memory using LZ4F_freeCompressionContext();
 */
LZ4FLIB_API LZ4F_errorCode_t LZ4F_createCompressionContext(LZ4F_cctx** cctxPtr, unsigned version);
LZ4FLIB_API LZ4F_errorCode_t LZ4F_freeCompressionContext(LZ4F_cctx* cctx);


/*----    Compression    ----*/

#define LZ4F_HEADER_SIZE_MAX 19   /* LZ4 Frame header size can vary from 7 to 19 bytes */
/*! LZ4F_compressBegin() :
 *  will write the frame header into dstBuffer.
 *  dstCapacity must be >= LZ4F_HEADER_SIZE_MAX bytes.
 * `prefsPtr` is optional : you can provide NULL as argument, all preferences will then be set to default.
 * @return : number of bytes written into dstBuffer for the header
 *           or an error code (which can be tested using LZ4F_isError())
 */
LZ4FLIB_API size_t LZ4F_compressBegin(LZ4F_cctx* cctx,
                                      void* dstBuffer, size_t dstCapacity,
                                      const LZ4F_preferences_t* prefsPtr);

/*! LZ4F_compressBound() :
 *  Provides minimum dstCapacity for a given srcSize to guarantee operation success in worst case scenarios.
 *  Estimation includes frame footer, which would be generated by LZ4F_compressEnd().
 *  Estimation doesn't include frame header, already generated by LZ4F_compressBegin().
 *  prefsPtr is optional : when NULL is provided, preferences will be set to cover worst case scenario.
 *  Result is always the same for a srcSize and prefsPtr, so it can be trusted to size reusable buffers.
 *  When srcSize==0, LZ4F_compressBound() provides an upper bound for LZ4F_flush() and LZ4F_compressEnd() operations.
 */
LZ4FLIB_API size_t LZ4F_compressBound(size_t srcSize, const LZ4F_preferences_t* prefsPtr);

/*! LZ4F_compressUpdate() :
 *  LZ4F_compressUpdate() can be called repetitively to compress as much data as necessary.
 *  An important rule is that dstCapacity MUST be large enough to ensure operation success even in worst case situations.
 *  This value is provided by LZ4F_compressBound().
 *  If this condition is not respected, LZ4F_compress() will fail (result is an errorCode).
 *  LZ4F_compressUpdate() doesn't guarantee error recovery. When an error occurs, compression context must be freed or resized.
 * `cOptPtr` is optional : NULL can be provided, in which case all options are set to default.
 * @return : number of bytes written into `dstBuffer` (it can be zero, meaning input data was just buffered).
 *           or an error code if it fails (which can be tested using LZ4F_isError())
 */
LZ4FLIB_API size_t LZ4F_compressUpdate(LZ4F_cctx* cctx, void* dstBuffer, size_t dstCapacity, const void* srcBuffer, size_t srcSize, const LZ4F_compressOptions_t* cOptPtr);

/*! LZ4F_flush() :
 *  When data must be generated and sent immediately, without waiting for a block to be completely filled,
 *  it's possible to call LZ4_flush(). It will immediately compress any data buffered within cctx.
 * `dstCapacity` must be large enough to ensure the operation will be successful.
 * `cOptPtr` is optional : it's possible to provide NULL, all options will be set to default.
 * @return : number of bytes written into dstBuffer (it can be zero, which means there was no data stored within cctx)
 *           or an error code if it fails (which can be tested using LZ4F_isError())
 */
LZ4FLIB_API size_t LZ4F_flush(LZ4F_cctx* cctx, void* dstBuffer, size_t dstCapacity, const LZ4F_compressOptions_t* cOptPtr);

/*! LZ4F_compressEnd() :
 *  To properly finish an LZ4 frame, invoke LZ4F_compressEnd().
 *  It will flush whatever data remained within `cctx` (like LZ4_flush())
 *  and properly finalize the frame, with an endMark and a checksum.
 * `cOptPtr` is optional : NULL can be provided, in which case all options will be set to default.
 * @return : number of bytes written into dstBuffer (necessarily >= 4 (endMark), or 8 if optional frame checksum is enabled)
 *           or an error code if it fails (which can be tested using LZ4F_isError())
 *  A successful call to LZ4F_compressEnd() makes `cctx` available again for another compression task.
 */
LZ4FLIB_API size_t LZ4F_compressEnd(LZ4F_cctx* cctx, void* dstBuffer, size_t dstCapacity, const LZ4F_compressOptions_t* cOptPtr);


/*-*********************************
*  Decompression functions
***********************************/
typedef struct LZ4F_dctx_s LZ4F_dctx;   /* incomplete type */
typedef LZ4F_dctx* LZ4F_decompressionContext_t;   /* compatibility with previous API versions */

typedef struct {
  unsigned stableDst;    /* pledge that at least 64KB+64Bytes of previously decompressed data remain unmodifed where it was decoded. This optimization skips storage operations in tmp buffers */
  unsigned reserved[3];  /* must be set to zero for forward compatibility */
} LZ4F_decompressOptions_t;


/* Resource management */

/*! LZ4F_createDecompressionContext() :
 *  Create an LZ4F_dctx object, to track all decompression operations.
 *  The version provided MUST be LZ4F_VERSION.
 *  The function provides a pointer to an allocated and initialized LZ4F_dctx object.
 *  The result is an errorCode, which can be tested using LZ4F_isError().
 *  dctx memory can be released using LZ4F_freeDecompressionContext();
 *  The result of LZ4F_freeDecompressionContext() is indicative of the current state of decompressionContext when being released.
 *  That is, it should be == 0 if decompression has been completed fully and correctly.
 */
LZ4FLIB_API LZ4F_errorCode_t LZ4F_createDecompressionContext(LZ4F_dctx** dctxPtr, unsigned version);
LZ4FLIB_API LZ4F_errorCode_t LZ4F_freeDecompressionContext(LZ4F_dctx* dctx);


/*-***********************************
*  Streaming decompression functions
*************************************/

/*! LZ4F_getFrameInfo() :
 *  This function extracts frame parameters (max blockSize, dictID, etc.).
 *  Its usage is optional.
 *  Extracted information is typically useful for allocation and dictionary.
 *  This function works in 2 situations :
 *   - At the beginning of a new frame, in which case
 *     it will decode information from `srcBuffer`, starting the decoding process.
 *     Input size must be large enough to successfully decode the entire frame header.
 *     Frame header size is variable, but is guaranteed to be <= LZ4F_HEADER_SIZE_MAX bytes.
 *     It's allowed to provide more input data than this minimum.
 *   - After decoding has been started.
 *     In which case, no input is read, frame parameters are extracted from dctx.
 *   - If decoding has barely started, but not yet extracted information from header,
 *     LZ4F_getFrameInfo() will fail.
 *  The number of bytes consumed from srcBuffer will be updated within *srcSizePtr (necessarily <= original value).
 *  Decompression must resume from (srcBuffer + *srcSizePtr).
 * @return : an hint about how many srcSize bytes LZ4F_decompress() expects for next call,
 *           or an error code which can be tested using LZ4F_isError().
 *  note 1 : in case of error, dctx is not modified. Decoding operation can resume from beginning safely.
 *  note 2 : frame parameters are *copied into* an already allocated LZ4F_frameInfo_t structure.
 */
LZ4FLIB_API size_t LZ4F_getFrameInfo(LZ4F_dctx* dctx,
                                     LZ4F_frameInfo_t* frameInfoPtr,
                                     const void* srcBuffer, size_t* srcSizePtr);

/*! LZ4F_decompress() :
 *  Call this function repetitively to regenerate compressed data from `srcBuffer`.
 *  The function will read up to *srcSizePtr bytes from srcBuffer,
 *  and decompress data into dstBuffer, of capacity *dstSizePtr.
 *
 *  The number of bytes consumed from srcBuffer will be written into *srcSizePtr (necessarily <= original value).
 *  The number of bytes decompressed into dstBuffer will be written into *dstSizePtr (necessarily <= original value).
 *
 *  The function does not necessarily read all input bytes, so always check value in *srcSizePtr.
 *  Unconsumed source data must be presented again in subsequent invocations.
 *
 * `dstBuffer` can freely change between each consecutive function invocation.
 * `dstBuffer` content will be overwritten.
 *
 * @return : an hint of how many `srcSize` bytes LZ4F_decompress() expects for next call.
 *  Schematically, it's the size of the current (or remaining) compressed block + header of next block.
 *  Respecting the hint provides some small speed benefit, because it skips intermediate buffers.
 *  This is just a hint though, it's always possible to provide any srcSize.
 *
 *  When a frame is fully decoded, @return will be 0 (no more data expected).
 *  When provided with more bytes than necessary to decode a frame,
 *  LZ4F_decompress() will stop reading exactly at end of current frame, and @return 0.
 *
 *  If decompression failed, @return is an error code, which can be tested using LZ4F_isError().
 *  After a decompression error, the `dctx` context is not resumable.
 *  Use LZ4F_resetDecompressionContext() to return to clean state.
 *
 *  After a frame is fully decoded, dctx can be used again to decompress another frame.
 */
LZ4FLIB_API size_t LZ4F_decompress(LZ4F_dctx* dctx,
                                   void* dstBuffer, size_t* dstSizePtr,
                                   const void* srcBuffer, size_t* srcSizePtr,
                                   const LZ4F_decompressOptions_t* dOptPtr);


/*! LZ4F_resetDecompressionContext() : added in v1.8.0
 *  In case of an error, the context is left in "undefined" state.
 *  In which case, it's necessary to reset it, before re-using it.
 *  This method can also be used to abruptly stop any unfinished decompression,
 *  and start a new one using same context resources. */
LZ4FLIB_API void LZ4F_resetDecompressionContext(LZ4F_dctx* dctx);   /* always successful */



#if defined (__cplusplus)
}
#endif

#endif  /* LZ4F_H_09782039843 */
/* end file /home/dev/Work/lz4/lib/lz4frame.h */
/* begin file /home/dev/Work/lz4/lib/lz4frame_static.h */
/*
   LZ4 auto-framing library
   Header File for static linking only
   Copyright (C) 2011-2016, Yann Collet.

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - LZ4 source repository : https://github.com/lz4/lz4
   - LZ4 public forum : https://groups.google.com/forum/#!forum/lz4c
*/

#ifndef LZ4FRAME_STATIC_H_0398209384
#define LZ4FRAME_STATIC_H_0398209384

#if defined (__cplusplus)
extern "C" {
#endif

/* lz4frame_static.h should be used solely in the context of static linking.
 * It contains definitions which are not stable and may change in the future.
 * Never use it in the context of DLL linking.
 *
 * Defining LZ4F_PUBLISH_STATIC_FUNCTIONS allows one to override this. Use at
 * your own risk.
 */
#ifdef LZ4F_PUBLISH_STATIC_FUNCTIONS
#define LZ4FLIB_STATIC_API LZ4FLIB_API
#else
#define LZ4FLIB_STATIC_API
#endif


/* ---   Dependency   --- */


/* ---   Error List   --- */
#define LZ4F_LIST_ERRORS(ITEM) \
        ITEM(OK_NoError) \
        ITEM(ERROR_GENERIC) \
        ITEM(ERROR_maxBlockSize_invalid) \
        ITEM(ERROR_blockMode_invalid) \
        ITEM(ERROR_contentChecksumFlag_invalid) \
        ITEM(ERROR_compressionLevel_invalid) \
        ITEM(ERROR_headerVersion_wrong) \
        ITEM(ERROR_blockChecksum_invalid) \
        ITEM(ERROR_reservedFlag_set) \
        ITEM(ERROR_allocation_failed) \
        ITEM(ERROR_srcSize_tooLarge) \
        ITEM(ERROR_dstMaxSize_tooSmall) \
        ITEM(ERROR_frameHeader_incomplete) \
        ITEM(ERROR_frameType_unknown) \
        ITEM(ERROR_frameSize_wrong) \
        ITEM(ERROR_srcPtr_wrong) \
        ITEM(ERROR_decompressionFailed) \
        ITEM(ERROR_headerChecksum_invalid) \
        ITEM(ERROR_contentChecksum_invalid) \
        ITEM(ERROR_frameDecoding_alreadyStarted) \
        ITEM(ERROR_maxCode)

#define LZ4F_GENERATE_ENUM(ENUM) LZ4F_##ENUM,

/* enum list is exposed, to handle specific errors */
typedef enum { LZ4F_LIST_ERRORS(LZ4F_GENERATE_ENUM) } LZ4F_errorCodes;

LZ4FLIB_STATIC_API LZ4F_errorCodes LZ4F_getErrorCode(size_t functionResult);



/**********************************
 *  Bulk processing dictionary API
 *********************************/
typedef struct LZ4F_CDict_s LZ4F_CDict;

/*! LZ4_createCDict() :
 *  When compressing multiple messages / blocks with the same dictionary, it's recommended to load it just once.
 *  LZ4_createCDict() will create a digested dictionary, ready to start future compression operations without startup delay.
 *  LZ4_CDict can be created once and shared by multiple threads concurrently, since its usage is read-only.
 * `dictBuffer` can be released after LZ4_CDict creation, since its content is copied within CDict */
LZ4FLIB_STATIC_API LZ4F_CDict* LZ4F_createCDict(const void* dictBuffer, size_t dictSize);
LZ4FLIB_STATIC_API void        LZ4F_freeCDict(LZ4F_CDict* CDict);


/*! LZ4_compressFrame_usingCDict() :
 *  Compress an entire srcBuffer into a valid LZ4 frame using a digested Dictionary.
 *  If cdict==NULL, compress without a dictionary.
 *  dstBuffer MUST be >= LZ4F_compressFrameBound(srcSize, preferencesPtr).
 *  If this condition is not respected, function will fail (@return an errorCode).
 *  The LZ4F_preferences_t structure is optional : you may provide NULL as argument,
 *  but it's not recommended, as it's the only way to provide dictID in the frame header.
 * @return : number of bytes written into dstBuffer.
 *           or an error code if it fails (can be tested using LZ4F_isError()) */
LZ4FLIB_STATIC_API size_t LZ4F_compressFrame_usingCDict(
    void* dst, size_t dstCapacity,
    const void* src, size_t srcSize,
    const LZ4F_CDict* cdict,
    const LZ4F_preferences_t* preferencesPtr);


/*! LZ4F_compressBegin_usingCDict() :
 *  Inits streaming dictionary compression, and writes the frame header into dstBuffer.
 *  dstCapacity must be >= LZ4F_HEADER_SIZE_MAX bytes.
 * `prefsPtr` is optional : you may provide NULL as argument,
 *  however, it's the only way to provide dictID in the frame header.
 * @return : number of bytes written into dstBuffer for the header,
 *           or an error code (which can be tested using LZ4F_isError()) */
LZ4FLIB_STATIC_API size_t LZ4F_compressBegin_usingCDict(
    LZ4F_cctx* cctx,
    void* dstBuffer, size_t dstCapacity,
    const LZ4F_CDict* cdict,
    const LZ4F_preferences_t* prefsPtr);


/*! LZ4F_decompress_usingDict() :
 *  Same as LZ4F_decompress(), using a predefined dictionary.
 *  Dictionary is used "in place", without any preprocessing.
 *  It must remain accessible throughout the entire frame decoding. */
LZ4FLIB_STATIC_API size_t LZ4F_decompress_usingDict(
    LZ4F_dctx* dctxPtr,
    void* dstBuffer, size_t* dstSizePtr,
    const void* srcBuffer, size_t* srcSizePtr,
    const void* dict, size_t dictSize,
    const LZ4F_decompressOptions_t* decompressOptionsPtr);


#if defined (__cplusplus)
}
#endif

#endif /* LZ4FRAME_STATIC_H_0398209384 */
/* end file /home/dev/Work/lz4/lib/lz4frame_static.h */
