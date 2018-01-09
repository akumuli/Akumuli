add_library(akumuli SHARED
    akumuli.cpp
    metadatastorage.cpp
    datetime.cpp
    log_iface.cpp
    util.cpp
    storage2.cpp
    crc32c.cpp
    status_util.cpp
    cursor.cpp
    index/stringpool.cpp
    index/seriesparser.cpp
    index/invertedindex.cpp
    storage_engine/blockstore.cpp
    storage_engine/volume.cpp
    storage_engine/nbtree.cpp
    storage_engine/compression.cpp
    storage_engine/column_store.cpp
    storage_engine/operators/operator.cpp
    storage_engine/operators/aggregate.cpp
    storage_engine/operators/scan.cpp
    storage_engine/operators/join.cpp
    storage_engine/operators/merge.cpp
    # query_processing
    query_processing/queryparser.cpp
    query_processing/queryplan.cpp
    queryprocessor.cpp
    queryprocessor_framework.cpp
    #hashfnfamily.cpp
    #anomalydetector.cpp
    saxencoder.cpp
    query_processing/rate.cpp
    query_processing/sax.cpp
    query_processing/top.cpp
    query_processing/scale.cpp
    query_processing/absolute.cpp
    query_processing/math.cpp
    #query_processing/anomaly.cpp
    #query_processing/filterbyid.cpp
    query_processing/spacesaver.cpp
    query_processing/limiter.cpp
    query_processing/sliding_window.cpp
    query_processing/queryparser.cpp
)

# Shared object version: "major.minor.micro"
# - micro updated every release when there is no API/ABI changes
# - minor updated when there are additions in API/ABI
# - major (ABI number) updated when there are changes in ABI (or removals)
set_target_properties(akumuli PROPERTIES
    SOVERSION "${APP_VERSION_MAJOR}"
    VERSION   "${APP_VERSION_MAJOR}.${APP_VERSION_MINOR}.${APP_VERSION_PATCH}")

include_directories(.)

include(CppcheckTargets)
add_cppcheck(akumuli UNUSED_FUNCTIONS STYLE POSSIBLE_ERROR FORCE)

# libakumuli

install(
    TARGETS
        akumuli
    LIBRARY DESTINATION
        ${CMAKE_INSTALL_LIBDIR}
)

install(
    FILES ../include/akumuli_version.h
	  ../include/akumuli_config.h
	  ../include/akumuli.h
	  ../include/akumuli_def.h
    DESTINATION
        ${CMAKE_INSTALL_INCLUDEDIR}
)

set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_PACKAGE_NAME "akumuli")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Evgeny Lazin")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Time-series storage engine")
set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

set(CPACK_PACKAGE_VERSION "${APP_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${APP_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${APP_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${APP_VERSION_PATCH}")

set(CPACK_PACKAGE_FILE_NAME
"akumuli_${APP_VERSION}-${PACKAGE_VERSION}_${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}")

# autogenerate dependency information
set (CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

include(CPack)

link_directories(${Boost_LIBRARY_DIRS})

if(!APPLE)
target_link_libraries(akumuli2
    "${SQLITE3_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
)
endif(!APPLE)
