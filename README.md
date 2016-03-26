README [![Build Status](https://travis-ci.org/akumuli/Akumuli.svg?branch=master)](https://travis-ci.org/akumuli/Akumuli)
======

[![Join the chat at https://gitter.im/akumuli/Akumuli](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akumuli/Akumuli?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

**Akumuli** is a numeric time-series database.
It can be used to capture, store and process time-series data in real-time.
The word "akumuli" can be translated from esperanto as "accumulate".

Disclaimer
----------

Akumuli is work in progress and not ready for production yet.


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

  `sudo apt-get install libapr1-dev libaprutil1-dev`

* Cmake:

  `sudo apt-get install cmake`

#### Building

1. `cmake .`
1. `make -j`

### Centos 7 / RHEL7 / Fedora?
#### Prerequisites
##### Semiautomatic
* RHEL has an old version of boost that didn't really support coroutines (It's 1.53, which should contain coroutines, but compiling fails). At the time of writing, Akumulid needs boost 1.54 PRECISELY, so uninstall the original and get version 1.54 from the boost website:
```
wget 'http://downloads.sourceforge.net/project/boost/boost/1.54.0/boost_1_54_0.tar.gz'
tar -xzvf boost_1_54_0.tar.gz
cd boost_1_54_0
./bootstrap.sh --prefix=/usr --libdir=/usr/lib64
./b2 -j4 
#Go get some coffee (-j4: Use four cores)...
./b2 install
#Go get some more coffee...
```
* We're assuming x86_64, otherwise, adapt libdir accordingly
* If there are errors `quadmath.h: No such file or directory`, do: `yum install libquadmath-devel`
* If there are errors `bzlib.h: No such file or directory`, do: `yum install bzip2-devel`
* If there are errors `pyconfig.h: No such file or directory`, do: `yum install python-devel`
* Then run `prerequisites.sh` to install the remaining libraries

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
[Trello board](https://trello.com/b/UO1sGA99)
