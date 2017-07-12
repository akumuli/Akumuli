Release notes
=============

Version 0.7.14

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
