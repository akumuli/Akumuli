add_executable(akumuli-tool main.cpp)

target_link_libraries(akumuli-tool
    akumuli
    ${LOG4CXX_LIBRARIES}
    ${APR_LIBRARY}
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
