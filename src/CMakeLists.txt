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
    internal_cursor.h
    compression.h
    storage.cpp
    page.cpp
    akumuli.cpp
    util.cpp
    sequencer.cpp
    cursor.cpp
    compression.cpp
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

