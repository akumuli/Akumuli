add_executable(akumulid main.cpp)
target_link_libraries(akumulid
    akumuli
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
