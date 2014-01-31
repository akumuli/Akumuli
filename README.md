README
======

**Akumuli** is a time-series database. The word "akumuli" can be translated from esperanto as "acumulate".

Rationale
---------

Most of the open source projects are focused on query language and things mostly suitable for web-analytics, but they ignores some serious problems:

* Dependency on third-party software.
* Timestamps, they doing it wrong!
* General purpose storage engines doesn't work well for time-series data (low write throughput).
* Impossible to use as embedded db.
* They can't use constant amount of disc space, like rrd-tool.

For example, OpenTSDB depends on Hadoop and HBase and can't be used in embedded scenario. RRD-tool can be used in embedded scenario and uses constant amount of disc space but has very slow and inefficient storage engine.

Few words about timestamps. Most systems rounds up timestamps up to some value (for example, OpenTSDB rounds every timestamp up to one second). This makes difficult or impossible to use this systems in process control domain. Worse that they works only with real timestamps (like UNIX time or UTC time). This makes impossible to use some other values as timestamps, for example, some sensors or ASICs can generate time-series data that contains sequence numbers that can be used as timestamps directly.

With akumuli I'm trying to solve this issues. Akumuli is embedded time-series database, without dependency on third-party software or services, that implements custom storage engine designed specifically for time series data.

Some characteristics of time series data
----------------------------------------

* High write throughput (millions of data-points per second)
* Many time series data sources are periodical
* Write depth is limited (very late writes can be dropped)

This characteristics can be used to 'cut corners' and optimize write and query performance.

First milestone goals
---------------------

* Writing
* Searching (cache-aware hybrid (interpolation and binary) searching)
* B-tree based in memory cache
* Compression for large entries
