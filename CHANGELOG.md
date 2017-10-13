Release notes
=============

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
