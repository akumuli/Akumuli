add_executable(akumulid
    main.cpp
    logger.cpp logger.h
)
target_link_libraries(akumulid
    akumuli
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
