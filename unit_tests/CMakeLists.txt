add_executable(ut_main
    test_sequencer.cpp
    test_page.cpp
    test_mmap.cpp
    test_sorting.cpp
    test_cursor.cpp
)
target_link_libraries(
    ut_main
    akumuli
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
add_test(akumuli_tests ut_main)
