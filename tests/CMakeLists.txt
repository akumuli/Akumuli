add_executable(main test_page.cpp)
target_link_libraries(main spatium ${Boost_LIBRARIES})
add_test(SpatiumTests main)
