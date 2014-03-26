add_executable(fill_test main.cpp)
target_link_libraries(fill_test
    akumuli
    ${LOG4CXX_LIBRARIES}
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
    libtbb.so
)
