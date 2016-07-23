add_library(akumuli SHARED
    ../include/akumuli_version.h
    ../include/akumuli_config.h
    ../include/akumuli.h
    ../include/akumuli_def.h
    storage_engine/volume.h
    storage_engine/volume.cpp
    storage_engine/compression.h
    storage_engine/compression.cpp
    storage_engine/blockstore.h
    storage_engine/blockstore.cpp
    storage_engine/nbtree.h
    storage_engine/nbtree.cpp
    status_util.cpp
    status_util.h
    log_iface.h
    storage.h
    page.h
    util.h
    sort.h
    sequencer.h
    cursor.h
    internal_cursor.h
    metadatastorage.h
    stringpool.h
    log_iface.cpp
    storage.cpp
    seriesparser.cpp
    page.cpp
    akumuli.cpp
    util.cpp
    sequencer.cpp
    cursor.cpp
    metadatastorage.cpp
    stringpool.cpp
    datetime.cpp
    buffer_cache.cpp
    anomalydetector.cpp
    saxencoder.cpp
    hashfnfamily.cpp
    invertedindex.cpp
    crc32c.cpp
    status_util.cpp
    # query_processing
    queryprocessor.cpp
    queryprocessor_framework.cpp
    query_processing/anomaly.cpp
    query_processing/sax.cpp
    query_processing/paa.cpp
    query_processing/filterbyid.cpp
    query_processing/randomsamplingnode.cpp
    query_processing/spacesaver.cpp
    query_processing/limiter.cpp
    # storage engine
    storage_engine/blockstore.cpp
    storage_engine/volume.cpp
    storage_engine/nbtree.cpp
    storage_engine/compression.cpp
    storage_engine/ingestion_engine.cpp
)

include_directories(.)

include(CppcheckTargets)
add_cppcheck(akumuli UNUSED_FUNCTIONS STYLE POSSIBLE_ERROR FORCE)

# libakumuli

install(
    TARGETS
        akumuli
    LIBRARY DESTINATION
        ${CMAKE_INSTALL_PREFIX}/lib
)

install(
    FILES ../include/akumuli_version.h
	  ../include/akumuli_config.h
	  ../include/akumuli.h
	  ../include/akumuli_def.h
    DESTINATION
        ${CMAKE_INSTALL_PREFIX}/include
)

set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_PACKAGE_NAME "akumuli")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Evgeny Lazin")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Time-series storage engine")
set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

set(CPACK_PACKAGE_VERSION "0.3.0")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "3")
set(CPACK_PACKAGE_VERSION_PATCH "0")

include(CPack)
