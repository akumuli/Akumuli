README [![Build Status](https://api.shippable.com/projects/5481624dd46935d5fbbf6b58/badge?branchName=master)](https://app.shippable.com/projects/5481624dd46935d5fbbf6b58/builds/latest)
======

[![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**Akumuli** is a numeric time-series database.
It can be used to capture, store and process time-series data in real-time.
The word "akumuli" can be translated from esperanto as "accumulate".


Features
-------

* Log-structured storage. 
* Row oriented at large scale, column oriented at small scale.
* Crash recovery.
* In-memory storage for recent data.
* Accepts unordered data (at some configurable extent).
* Real-time compression (up to 2.5 bytes per element on appropriate data).
* Simple query language based on JSON over HTTP.
* Query results returned using chunked transfer encoding at rate about 50MB/second (about 1M data points/second).
* Series are organized using metrics and tags.
* Time-series can be grouped by time (find aggregate for each 5sec interval).
* Time-series can be joined together by tags.
* Resampling (PAA transform), sliding window methods.
* Random sampling.
* Frequent items and heavy hitters.
* SAX transformation.
* Anomaly detection (SMA, EWMA, Holt-Winters).
* Data ingestion through TCP or UDP based protocols (over 1M data points/second).
* Continuous queries (streaming).

 
Documentation
-------------
* [Wiki](https://github.com/akumuli/Akumuli/wiki)
* [Getting started](https://github.com/akumuli/Akumuli/wiki/Getting-started)

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
[Trello board](https://trello.com/b/UO1sGA99)
