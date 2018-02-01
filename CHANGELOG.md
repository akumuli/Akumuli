Release notes
=============

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
