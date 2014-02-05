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

A few words about timestamps. Most systems round timestamps up to some value (for example, OpenTSDB rounds every timestamp up to one second). This makes it difficult or impossible to use these systems in process control domain. Worse than that, they only work with real timestamps (like UNIX time or UTC time). It means one can't use some other values as timestamps. For example, some sensors or ASICs can generate time-series data that contains sequence numbers that can be used as timestamps directly.

With akumuli I'm trying to solve these issues. Akumuli is embedded time-series database, without dependency on third-party software or services, that implements custom storage engine designed specifically for time series data.

Some characteristics of time series data
----------------------------------------

* High write throughput (millions of data-points per second)
* Many time series data sources are periodical
* Write depth is limited (very late writes can be dropped)

These characteristics can be used to "cut corners" and optimize write and query performance.

First milestone goals
---------------------

* Writing
* Searching (cache-aware hybrid (interpolation and binary) searching)
* B-tree located in memory cache
* Compression for large entries
