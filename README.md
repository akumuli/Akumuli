README
======

**Akumuli** is a time-series database. The word "akumuli" can be translated from esperanto as "accumulate".


Rationale
---------

Most open source projects focus on query language and things useful for web-analytics, but they ignore some serious problems:

* Dependencies on third-party software.
* As a consequence, it's impossible to use them as embedded database.
* Timestamps: they're doing it wrong!
* General purpose storage engines don't work well for time-series data (low write throughput).
* They can't fit in constant amount of disk space, like RRD-tool.

For example, OpenTSDB depends on Hadoop and HBase and can't be used in embedded scenario. RRD-tool can be used in embedded scenario and uses constant amount of disk space, but has very slow and inefficient storage engine.

Most systems round timestamps up to some value (for example, OpenTSDB rounds every timestamp up to one second). This makes it difficult or impossible to use these systems in process control domain. Worse than that, they only work with real timestamps (like UNIX time or UTC time). It means one can't use some other values as timestamps. For example, some sensors or ASICs can generate time-series data that contains sequence numbers that can be used as timestamps directly.

With akumuli I'm trying to solve these issues. Akumuli is embedded time-series database, without dependency on third-party software or services, that implements custom storage engine designed specifically for time series data.

Some characteristics of time series data
----------------------------------------

* High write throughput (millions of data-points per second)
* Many time series data sources are periodical
* Write depth is limited (very late writes can be dropped)

These characteristics can be used to "cut corners" and optimize write and query performance.

Features
--------
* Implemented as dynamic C library
* Memory mapped
* x64 only
* Uses constant amount of disk space (like RRD-tool)
* Crash recovery
* Very hight write thorowghput
* Allows unordered writes
* Compressed (up to 3x)
* Interpolation search and fast range scans
 
Documentation
-------------
* [Wiki](https://github.com/akumuli/Akumuli/wiki)
* [Getting started](https://github.com/akumuli/Akumuli/wiki/Getting-started)
* [API reference](https://github.com/akumuli/Akumuli/wiki/API-Reference)

How to build
------------

### Ubuntu

#### Prerequisites

##### Automatic

* Run `prerequisites.sh`. It will try to do the best thing.

##### Manual

In case automatic script didn't work:

* Boost:

  `sudo apt-get install libboost-dev libboost-system-dev libboost-thread-dev libboost-filesystem-dev libboost-test-dev libboost-coroutine-dev libboost-context-dev`
  
* APR:

  `sudo apt-get install libapr1`

* Cmake:

  `sudo apt-get install cmake`

#### Building

1. `cmake .`
1. `make -j`

Questions?
----------
[Google group](https://groups.google.com/forum/#!forum/akumuli)
