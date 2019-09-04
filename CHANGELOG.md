Release notes
=============

Version 0.8.70
--------------

IMPROVEMENT

* Implement events filtering

BUG FIX

* Fix materializaton step for event

Version 0.8.69
--------------

IMPROVEMENT

* Add --config command line argument

BUG FIX

* Fix framing issue in RESP protocol parser

Version 0.8.68
--------------

IMPROVEMENT

* Support 32-bit ARM/Intel processors

BUG FIX

* Fix #312 integer overflow in RESP parser
* Prohibit use of WAL with single volume

Version 0.8.67
--------------

IMPROVEMENT

* Introduce event storage 

Version 0.8.66
--------------

IMPROVEMENT

* Improve memory requirements by using vector I/O 

Version 0.8.65
--------------

IMPROVEMENT

* Introduce pivot-by-tag and group-by-tag fields
* Depricate group-by field
* Group-aggregate query can be used with pivot/group-by-tag

Version 0.8.64
--------------

IMPROVEMENT

* Aggregate query can fetch several metrics at time
* Group-aggregate query can fetch several metrics at time
* Aggregate query output contains name of the function

BUG FIX

* Fix #296 time-duration overflow in query parser

Version 0.8.63
--------------

BUG FIX

* Fix fallocate block size

Version 0.8.61
--------------

BUG FIX

* Fix race condition in WAL

Version 0.8.60
--------------

BUG FIX

* Fix buffer overflow in WAL

Version 0.8.59
--------------

BUG FIX

* Fix incorrect ISO 8601 date-time parsing

Version 0.8.58
--------------

IMPROVEMENT

* Support durable writes
* Add write-ahead log implementation
* Implement database recovery using log replay

Version 0.7.57
--------------

BUG FIX

* Fix select query behavior when query boundary crosses the retention boundary
* Fix aggregate query precision error on retention boudnary

Version 0.7.56
--------------

BUG FIX

* Fix bug in database open procedure
* Fix panic on database close


Version 0.7.55
--------------

BUG FIX

* Fix timestamps in group-aggregate query


Version 0.7.54
--------------

BUG FIX

* Disable superblock compression
* Fix recovery with lost metadata

Version 0.7.53
--------------

IMPROVEMENT

* Detailed error messages
* Escape sequences in series names

Version 0.7.52
--------------

BUG FIX

* Fix obsolete tree handling

Version 0.7.51
--------------

IMPROVEMENT

* Improve memory efficiency

Version 0.7.50
--------------

IMPROVEMENT

* ARM support

Version 0.7.49
--------------

IMPROVEMENT

* Build packages for multiple platforms

Version 0.7.48
--------------

BUG FIX

* Fix recovery error
* Use robust recovery algorithm implementation

Version 0.7.47
--------------

BUG FIX

* Fix packagecloud deployment

Version 0.7.46
--------------

IMPROVEMENT

* Introduce OSX support

Version 0.7.45
--------------

IMPROVEMENT

* Minimize stdout logging in docker container
* Docker image versioning

Version 0.7.44
--------------

IMPROVEMENT

* Dictionary support in RESP protocol

Version 0.7.43
--------------

IMPROVEMENT

* Log to stdout/stderr from docker container

Version 0.7.42
--------------

BUG FIX

* Fix UDP-server error handling

Version 0.7.41
--------------

BUG FIX

* Fix crash on random-device initialization failure

Version 0.7.40
--------------

IMPROVEMENT

* Filter clause work with group-aggregate query

Version 0.7.39
--------------

IMPROVEMENT

* Add filter query

Version 0.7.38
--------------

IMPROVEMENT

* Allow libakumuli.so to be loaded by dlopen

Version 0.7.37
--------------

BUG FIX

* Fix UDP server memory corruption #243

Version 0.7.36
--------------

BUG FIX

* Fix packagecloud deploy

Version 0.7.35
--------------

IMPROVEMENTS

* Provide generic and optimized images

Version 0.7.34
--------------

BUG FIXES

* Signals are not propagated to the akumulid server in docker container

Version 0.7.33
--------------

IMPROVEMENTS

* Stop daemon using both SIGINT and SIGTERM

Version 0.7.32
--------------

IMPROVEMENTS

* Specify version for shared object for easier ABI checking

Version 0.7.31
--------------

BUG FIXES

* Fix #235 (don't use -march=native)

Version 0.7.30
--------------

IMPROVEMENTS

* Use CMake macros for install directories from GNUInstallDirs

Version 0.7.29
--------------

BUG FIXES

* Add the missing header for GCC 7.2.x

Version 0.7.28
--------------

IMPROVEMENTS

* Update prerequisites.sh

Version 0.7.27
--------------

BUG FIXES

* Set up locale (fix #227)

Version 0.7.26
--------------

NEW FEATURES

* Build Docker container on CI

Version 0.7.25
--------------

BUG FIXES

* Fix idle CPU load problem

Version 0.7.24
--------------

BUG FIXES

* Fix debian package dependencies

Version 0.7.23
--------------

NEW FEATURES

* New form of 'where' clause added

Version 0.7.22
--------------

BUG FIXES

* Rate calculation is fixed

Version 0.7.21
--------------

NEW FEATURES

* Tranformation pipleline added to query processor
* A bunch of new functions added to the pipeline
* New endpoint added - /api/function-names

Version 0.7.20
--------------

BUG FIXES

* Group-aggregate query failure is fixed (#208).

Version 0.7.19
--------------

NEW FEATURES

* New search API
* New suggest API (for Grafana plugin)

IMPROVEMENTS

* Inverted index for series names

Version 0.7.18
--------------

IMPROVEMENTS

* OpenTSDB protocol improved. TCollector can send data to Akumuli now.

Version 0.7.17
--------------

IMPROVEMENTS

* Node-split algorithm added to NB-tree. This algorithm can be used as a building
  block for the update/delete implementation.

Version 0.7.16
--------------

BUG FIXES

* Panic on open is fixed (#193).
* Join query error on incomplete data is fixed (#200).

IMPROVEMENTS

* New join query implementation handles missing data properly, join query can be
  used even if data poins are not perfectly aligned in time.

Version 0.7.15
--------------

BUG FIXES

* Race condition in HTTP server that prevented server from sending error response

Version 0.7.14
--------------

NEW FEATURES

* OpenTSDB telnet-style API implemented (partially)

BUG FIXES

* Join query error fixed #194
* Name truncation error #197 is fixed

Version 0.7.13
--------------

IMPROVEMENTS

* Blockstore uses mmap to read data from disk if possible
* Blockstore uses zero-copy mechanism for mmap data

Version 0.7.12
--------------

IMPROVEMENTS

* Blockstore is not using metadata file to track volume information (issue #177)
* Volume information is stored in the sqlite3 database now (with recovery information)

Version 0.7.x
-------------

NEW FEATURES

* New storage engine based on NB+tree (B+tree variant) with crash recovery
* Blockstore implementation added (fixed size and extendable)
* HTTP-server with JSON based search API
* Ingestion through TCP and UDP
* REST based ingestion format
* Bulk data ingestion
* Query API
  - Metadata query ("select meta")
  - Scan query ("select" statement)
  - Aggregations ("aggregate" statement)
  - Group-by on series names ("group-by" statement)
  - Group-aggregate on data points ("group-aggregate" statement)
  - Join query ("join" statement)
* Column-store with query planner with support for different materialization strategies
  and operators
