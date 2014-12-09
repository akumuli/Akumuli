# Main executable
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

######################
#     Test suite     #
######################

# Protocol parser
add_executable(test_protocolparser
    test_protocolparser.cpp
    protocolparser.cpp protocolparser.h
)
target_link_libraries(test_protocolparser
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
add_test(protocol-parser test_protocolparser)

