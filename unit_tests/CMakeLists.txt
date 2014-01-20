add_executable(ut_main
    test_page.cpp
    test_mmap.cpp
    test_sorting.cpp
    test_cache.cpp
)
message("log4cxx libs: ${LOG4CXX_LIBRARIES}")
target_link_libraries(
    ut_main
    akumuli
    ${LOG4CXX_LIBRARIES}
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
)
add_test(akumuli_tests main)
