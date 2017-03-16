README [![Build Status](https://travis-ci.org/akumuli/Akumuli.svg?branch=master)](https://travis-ci.org/akumuli/Akumuli) [![Coverity Scan Build Status](https://scan.coverity.com/projects/8879/badge.svg)](https://scan.coverity.com/projects/akumuli) [![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
======

**Akumuli** is a numeric time-series database.
It can be used to capture, store and process time-series data in real-time.
The word "akumuli" can be translated from esperanto as "accumulate".

Features
-------

* Column-oriented time-series database.
* Log-structured append-only B+tree with multiversion concurrency control.
* Crash safety.
* Fast aggregation without pre-configured rollups or materialized views.
* Custom compression algorithm (dictionary + entropy) with small memory overhead (about 2.5 bytes per element on appropriate data).
* Compressed in-memory storage for recent data.
* Can be used as a server application or an embedded library.
* Simple query language based on JSON over HTTP.
* Fast data ingestion over the network:
  * 4.5M data points per second on 8-core Intel Xeon E5-2670 v2 (m3.2xlarge EC2 instance).
  * 16.1M data points per second on 32-core Intel Xeon E5-2680 v2 (c3.8xlarge EC2 instance).
* Query results returned using chunked transfer encoding at rate about 50MB/second (about 1M data points/second) per core.

Documentation
-------------
* [Wiki](https://github.com/akumuli/Akumuli/wiki)
* [Getting started](https://github.com/akumuli/Akumuli/wiki/Getting-started)

How to build
------------

### Ubuntu / Debian

#### Prerequisites

##### Automatic

* Run `prerequisites.sh`. It will try to do the best thing.

##### Manual

In case automatic script didn't work:

* Boost:

  `sudo apt-get install libboost-all-dev`

* log4cxx:

  `sudo apt-get install log4cxx`

* jemalloc:

  `sudo apt-get install libjemalloc-dev`

* microhttpd:

  `sudo apt-get install libmicrohttpd-dev`

* APR:

  `sudo apt-get install libapr1-dev libaprutil1-dev libaprutil1-dbd-sqlite3`

* SQLite:

  `sudo apt-get install libsqlite3-dev`

* Cmake:

  `sudo apt-get install cmake`

#### Building

1. `cmake .`
1. `make -j`


### Centos 7 / RHEL7 / Fedora

##### Automatic

* Run `prerequisites.sh`. It will try to do the best thing.

##### Manual

In case automatic script didn't work:

* Boost:

  `sudo yum install boost boost-devel`

* log4cxx:

  `sudo yum install log4cxx log4cxx-devel`

* jemalloc:

  `sudo yum install jemalloc-devel`

* microhttpd:

  `sudo yum install libmicrohttpd-devel`

* APR:

  `sudo yum install apr-devel apr-util-devel apr-util-sqlite`

* SQLite

  `sudo yum install sqlite sqlite-devel`

* Cmake:

  `sudo yum install cmake`


#### Building

1. `cmake .`
1. `make -j`
1. `make`

### Centos 6 / RHEL6

#### Prequisites

* Same as for RHEL7, but we need to manually install log4cxx, as there isn't a package in the repos:
```
wget http://www.pirbot.com/mirrors/apache/logging/log4cxx/0.10.0/apache-log4cxx-0.10.0.tar.gz
tar -xzvf apache-log4cxx-0.10.0.tar.gz 
cd apache-log4cxx-0.10.0
```
* Add `#include <cstring>` to: `src/main/cpp/inputstreamreader.cpp`, `src/main/cpp/socketoutputstream.cpp` and `src/examples/cpp/console.cpp`
* Add `#include <cstdio>` to: `src/examples/cpp/console.cpp`
```
./configure --prefix=/usr --libdir=/usr/lib64
make -j4
sudo make install
```
* Go on as for RHEL7

Questions?
----------

[Google group](https://groups.google.com/forum/#!forum/akumuli)
