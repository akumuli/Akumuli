Akumuli project changelog
=========================

0.2.0
=====
Storage system:
---------------
- different updates to simplify building and dependency management
  - remove dependency on boost.coroutine and boost.context
  - newest version of boost (1.54 or higher) is not required anymore, library can be build on ubuntu 12.04
  - separate tests runners for different components for better reporting and code quality
- API updates
  - `aku_create_database` optional parameters now passed by value, not by pointer

----------------------------  

0.1.0
=====
Storage system:
---------------
- better C-compatibility
- tuning parameters and storage options revamp

