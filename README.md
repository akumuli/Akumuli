README
======

**Akumuli** is a time-series storage, optimized specificaly for time series data. Akumuli can be translated from esperanto as `acumulate`.
Goal of the project - creation of the new time series storage solution with specific properties. Akumuli must supprot
very large files, more than 4Gb. Akumuli storage is round-robin database (like RRD-tool but more advanced) that uses constant amount of disc space. It
support 64-bit timestamps and doesn't round them up to second or milisecond. Akumuli is a library with simple "C" API.
Akumuli doesn't depend on any 3rd party SQL or Key-Value storage.


Some characteristics of time series data
----------------------------------------
    * Higth write throughput (millions of data-points per second);
    * Many time series data sources are periodical;
    * Most of the writes are from current time with some bias;

This characteristics can be used to 'cut corners' and optimize write and query performance.

First milestone goals
---------------------
    * Writing
    * Searching
    * In memory cache
    * Compression
    * Configurable data redundancy
