Akumuli project changelog
=========================

0.2.0
=====
Storage system:
---------------
- Separate tests runners for different components for better reporting and code quality
- API updates
  - `aku_create_database` optional parameters now passed by value, not by pointer
  - new open parameters:
    - durability - allows to trade some durability for speed (default is max durability)
    - enable_huge_tlb - allows to enable or diable huge tlb in memory mapped files (0 - disabled)

----------------------------  

0.1.0
=====
Storage system:
---------------
- better C-compatibility
- tuning parameters and storage options revamp

