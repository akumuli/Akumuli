add_executable(ut_main
    test_page.cpp
    test_mmap.cpp
    test_sorting.cpp
    test_cache.cpp
    test_cursor.cpp
)
message("log4cxx libs: ${LOG4CXX_LIBRARIES}")
message("log4cxx libs: ${Boost_LIBRARIES}")
target_link_libraries(
    ut_main
    akumuli
    ${LOG4CXX_LIBRARIES}
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
    libtbb.so
)
add_test(akumuli_tests ut_main)
