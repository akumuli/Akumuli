README [![Build Status](https://api.shippable.com/projects/5481624dd46935d5fbbf6b58/badge?branchName=master)](https://app.shippable.com/projects/5481624dd46935d5fbbf6b58/builds/latest)
======

[![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**Akumuli** is a numeric time-series database. The word "akumuli" can be translated from esperanto as "accumulate".


Rationale
---------

Most open source projects focus on query language and things useful for web-analytics, but they ignore some key characteristics of time series data:

* High write throughput (millions of data-points per second)
* Very late writes can be dropped
* Numeric time-series can be compressed very efficiently
* Periodic time-series can be compressed very efficiently
* Compression is crucial for time-series storage!

Features
--------
* Implements specialized storage engine for time-series data
* Memory mapped and x64 only
* Uses constant amount of disk space (like RRD-tool)
* Crash recovery 
* Very high write throughput (about 1M writes per second on single machine)
* Allows unordered writes
* Compressed (specialized compression algorithms for different data elements - timestamps, ids, values)
* Easy to use server software (based on Redis protocol)
 
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

  `sudo apt-get install libapr1-dev libaprutil1-dev

* Cmake:

  `sudo apt-get install cmake`

#### Building

1. `cmake .`
1. `make -j`

Questions?
----------
[Google group](https://groups.google.com/forum/#!forum/akumuli)
