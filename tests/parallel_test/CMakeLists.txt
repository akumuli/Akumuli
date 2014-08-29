add_executable(parallel_test main.cpp)
target_link_libraries(parallel_test
    akumuli
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
