README [![Build Status](https://travis-ci.org/akumuli/Akumuli.svg?branch=master)](https://travis-ci.org/akumuli/Akumuli) [![Coverity Scan Build Status](https://scan.coverity.com/projects/8879/badge.svg)](https://scan.coverity.com/projects/akumuli) [![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
======

**Akumuli** is a time-series database for modern hardware. 
It can be used to capture, store and process time-series data in real-time. 
The word "akumuli" can be translated from Esperanto as "accumulate".

Features
-------

* Column-oriented storage.
* Based on novel [LSM and B+tree hybrid datastructure](http://akumuli.org/akumuli/2017/04/29/nbplustree/) with multiversion concurrency control (no concurrency bugs, parallel writes, optimized for SSD and NVMe).
* Supports both metrics and arbitrary events.
* Fast and effecient compression algorithm that outperforms 'Gorilla' time-series compression.
* Crash safety and recovery.
* Fast aggregation without pre-configured rollups or materialized views.
* Many queries can be executed without decompressing the data.
* Compressed in-memory storage for recent data.
* Can be used as a server application or embedded library.
* Simple API based on JSON and HTTP.
* Fast range scans and joins, read speed doesn't depend on database cardinality.
* Fast data ingestion:
  * 5.4M writes/sec on DigitalOcean droplet with 8-cores 32GB of RAM (using only 6 cores)
  * 4.6M writes/sec on DigitalOcean droplet with 8-cores 32GB of RAM (6 cores with enabled WAL)
  * 16.1M writes/sec on 32-core Intel Xeon E5-2680 v2 (c3.8xlarge EC2 instance).
* Queries are executed lazily. Query results are produced as long as client reads them.
* Compression algorithm and input parsers are fuzz-tested on every code change.
* Grafana [datasource](https://github.com/akumuli/akumuli-datasource) plugin.
* Fast and compact inverted index for time-series lookup.


Roadmap
------

|Storage engine features        |Current version|Future versions|
|-------------------------------|---------------|---------------|
|Inserts                        |In order       |Out of order   |
|Updates                        |-              |+              |
|Deletes                        |-              |+              |
|MVCC                           |+              |+              |
|Compression                    |+              |+              |
|Tags                           |+              |+              |
|High-throughput ingestion      |+              |+              |
|High cardinality               |+              |+              |
|Crash recovery                 |+              |+              |
|Incremental backup             |-              |+              |
|Clustering                     |-              |+              |
|Replication                    |-              |+              |
|ARM support                    |+              |+              |
|Windows support                |-              |+              |

|Query language features        |Current version|Future versions|
|-------------------------------|---------------|---------------|
|Range scans                    |+              |+              |
|Merge series                   |+              |+              |
|Aggregate series               |+              |+              |
|Merge & aggregate              |+              |+              |
|Group-aggregate                |+              |+              |
|Group-aggregate & merge        |+              |+              |
|Join                           |+              |+              |
|Join & merge                   |-              |+              |
|Join & group-aggregate         |-              |+              |
|Join & group-aggregate & merge |-              |+              |
|Filter by value                |+              |+              |
|Filter & group-aggregate       |+              |+              |
|Filter & join                  |+              |+              |



Supported Architectures
-----------------------

Akumuli supports 64 and 32-bit Intel processors. It also works on 64 and 32-bit ARM processors but these architectures are not covered by continous integration.

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

* AMD 64 Ubuntu 14.04
* AMD 64 Ubuntu 16.04
* AMD 64 Ubuntu 18.04
* AMD 64 Debian Jessie
* AMD 64 Debian Stretch
* AMD 64 CentOS 7
* ARM 64 Ubuntu 16.04
* ARM 64 Ubuntu 18.04
* ARM 64 CentOS 7

Docker image is availabe through [Docker Hub](https://hub.docker.com/r/akumuli/akumuli/tags/).

Tools for monitoring
--------------------

Akumuli supports OpenTSDB telnet-style API for writing. This means that many collectors works with it
without any trouble, for instance `netdata`, `collectd`, and `tcollector`. Grafana
[datasource](https://github.com/akumuli/akumuli-datasource) plugin is availabe as well.
Akumuli can be used as a long-term storage for Prometheus using [akumuli-prometheus-adapter](https://github.com/akumuli/akumuli-prometheus-adapter).

[Google group](https://groups.google.com/forum/#!forum/akumuli)
