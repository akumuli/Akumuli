add_executable(bench main.cpp)
target_link_libraries(bench
    #rt
    akumuli
    ${LOG4CXX_LIBRARIES}
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
