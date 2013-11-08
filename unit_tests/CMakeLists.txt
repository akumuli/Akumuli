add_executable(main test_page.cpp)
target_link_libraries(main akumuli ${Boost_LIBRARIES})
add_test(akumuli_tests main)
