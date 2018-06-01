README [![Build Status](https://travis-ci.org/akumuli/Akumuli.svg?branch=master)](https://travis-ci.org/akumuli/Akumuli) [![Coverity Scan Build Status](https://scan.coverity.com/projects/8879/badge.svg)](https://scan.coverity.com/projects/akumuli) [![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
======

**Akumuli** is a time-series database for modern hardware. 
It can be used to capture, store and process time-series data in real-time. 
The word "akumuli" can be translated from Esperanto as "accumulate".

Features
-------

* True column-oriented format (not PAX).
* Based on novel [LSM and B+tree hybrid datastructure](http://akumuli.org/akumuli/2017/04/29/nbplustree/) with multiversion concurrency control (no concurrency bugs, parallel writes, optimized for SSD and NVMe).
* Crash safety and recovery.
* Fast aggregation without pre-configured rollups or materialized views.
* Queries can be executed without decompressing the data.
* Fast compression algorithm (dictionary + entropy) with small memory overhead (about 2.5 bytes per element on appropriate data).
* Compressed in-memory storage for recent data.
* Can be used as a server application or an embedded library.
* Simple query language based on JSON and HTTP.
* Fast range scans and joins, read speed doesn't depend on database cardinality.
* Fast data ingestion over the network:
  * 4.5M data points per second on 8-core Intel Xeon E5-2670 v2 (m3.2xlarge EC2 instance).
  * 16.1M data points per second on 32-core Intel Xeon E5-2680 v2 (c3.8xlarge EC2 instance).
* Query results are streamed to client using the chunked transfer encoding of the HTTP protocol.
* Decompression algorithm and input parsers were fuzz-tested.
* Grafana [datasource](https://github.com/akumuli/akumuli-datasource) plugin.
* Fast and compact inverted index for series lookup.


|Storage engine features        |Current version|Future versions|
|-------------------------------|---------------|---------------|
|Inserts                        |In order       |Out of order   |
|Updates                        |-              |+              |
|Deletes                        |-              |+              |
|MVCC                           |+              |+              |
|Compression                    |+              |+              |
|Tags                           |+              |+              |
|High-throughput ingestion      |+              |+              |
|High cardinality               |-              |+              |
|Crash recovery                 |+              |+              |
|Incremental backup             |-              |+              |
|Clustering                     |-              |+              |
|Replication                    |-              |+              |
|ARM support                    |-              |+              |
|Windows support                |-              |+              |

|Query language features        |Current version|Future versions|
|-------------------------------|---------------|---------------|
|Range scans                    |+              |+              |
|Merge series                   |+              |+              |
|Aggregate series               |+              |+              |
|Merge & aggregate              |+              |+              |
|Group-aggregate                |+              |+              |
|Group-aggregate & merge        |-              |+              |
|Join                           |+              |+              |
|Join & merge                   |-              |+              |
|Join & group-aggregate         |-              |+              |
|Join & group-aggregate & merge |-              |+              |
|Filter by value                |+              |+              |
|Filter & group-aggregate       |+              |+              |
|Filter & join                  |+              |+              |

Gettings Started
----------------
* You can find [documentation](https://akumuli.gitbook.io/docs) here
* [Installation & build instructions](https://akumuli.gitbook.io/docs/getting-started)
* [Getting started guide](https://akumuli.gitbook.io/docs/getting-started)
* [Writing data](https://akumuli.gitbook.io/docs/writing-data)

Supported Platforms
-------------------

Pre-built [Debian/RPM packages](https://packagecloud.io/Lazin/Akumuli) for the following platforms
are available via packagecloud:

* Ubuntu 14.04
* Ubuntu 16.04
* Debian Jessie
* Debian Stretch
* CentOS 7

Docker image is availabe through [Docker Hub](https://hub.docker.com/r/akumuli/akumuli/tags/).

Tools for monitoring
--------------------

Akumuli supports OpenTSDB telnet-style API for writing. This means that many collectors works with it
without any trouble, for instance `netdata`, `collectd`, and `tcollector`. Grafana
[datasource](https://github.com/akumuli/akumuli-datasource) plugin is availabe as well.

[Google group](https://groups.google.com/forum/#!forum/akumuli)
