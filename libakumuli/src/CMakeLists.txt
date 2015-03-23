add_library(akumuli SHARED
    ../include/akumuli_version.h
    ../include/akumuli_config.h
    ../include/akumuli.h
    ../include/akumuli_def.h
    storage.h
    page.h
    util.h
    sort.h
    sequencer.h
    cursor.h
    compression.h
    compression.cpp
    internal_cursor.h
    compression.h
    metadatastorage.h
    stringpool.h
    storage.cpp
    seriesparser.cpp
    page.cpp
    akumuli.cpp
    util.cpp
    sequencer.cpp
    cursor.cpp
    metadatastorage.cpp
    queryprocessor.cpp
    stringpool.cpp
)

include_directories(../include)

add_executable(
    test_page
        test_page.cpp
        page.cpp
        cursor.cpp
        util.cpp
        compression.cpp
)

target_link_libraries(
    test_page
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
    pthread
)

add_test(page test_page)

add_executable(
    test_sequencer
        test_sequencer.cpp
        sequencer.cpp
        cursor.cpp
        page.cpp
        util.cpp
        compression.cpp
)

target_link_libraries(
    test_sequencer
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
    pthread
)

add_test(sequencer test_sequencer)

add_executable(
    test_cursor
        test_cursor.cpp
        cursor.cpp
        page.cpp
        util.cpp
        compression.cpp
)

target_link_libraries(
    test_cursor
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
    pthread
)

add_test(cursor test_cursor)

add_executable(
    test_mmap
        test_mmap.cpp
        util.cpp
)

target_link_libraries(
    test_mmap
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)

add_test(mmap test_mmap)

add_executable(
    test_compression
        test_compression.cpp
        compression.cpp
)

target_link_libraries(
    test_compression
    ${Boost_LIBRARIES}
)

add_test(compression test_compression)

# Test storage

add_executable(
    test_storage
        test_storage.cpp
        storage.cpp
        seriesparser.cpp
        page.cpp
        util.cpp
        compression.cpp
        cursor.cpp
        sequencer.cpp
        akumuli.cpp
        metadatastorage.cpp
        queryprocessor.cpp
        stringpool.cpp
)

target_link_libraries(
    test_storage
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)

add_test(storage test_storage)

# Test series parser

add_executable(
    test_seriesparser
    seriesparser.cpp
    stringpool.cpp
    test_parser.cpp
    queryprocessor.cpp
    util.cpp
)

target_link_libraries(
    test_seriesparser
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)

add_test(seriesparser test_seriesparser)

# Series parser perftest

add_executable(
    perf_seriesmatcher
    seriesparser.cpp
    stringpool.cpp
    perf_seriesmatcher.cpp
    queryprocessor.cpp
    util.cpp
)

target_link_libraries(
    perf_seriesmatcher
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)

# Test query processor

add_executable(
    test_queryprocessor
    queryprocessor.cpp
    stringpool.cpp
    test_queryprocessor.cpp
    util.cpp
)

target_link_libraries(
    test_queryprocessor
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)

add_test(queryprocessor test_queryprocessor)

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

set(CPACK_PACKAGE_VERSION "0.1.0")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "1")
set(CPACK_PACKAGE_VERSION_PATCH "0")

include(CPack)
