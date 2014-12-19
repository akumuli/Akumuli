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

# Streams
add_executable(test_streams
    test_stream.cpp
    stream.cpp stream.h
)
target_link_libraries(test_streams
    ${Boost_LIBRARIES}
)
add_test(streams test_streams)

# RESP
add_executable(test_respstream
    test_respstream.cpp
    stream.cpp stream.h
    resp.cpp resp.h
)
target_link_libraries(test_respstream
    ${Boost_LIBRARIES}
)
add_test(respstream test_respstream)

# RESP perf test
add_executable(perf_respstream
    perf_respstream.cpp
    stream.cpp stream.h
    resp.cpp resp.h
)
target_link_libraries(perf_respstream
    ${Boost_LIBRARIES}
)

# Protocol parser
add_executable(test_protocolparser
    test_protocolparser.cpp
    protocolparser.cpp protocolparser.h
    stream.cpp stream.h
    resp.cpp resp.h
)
target_link_libraries(test_protocolparser
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
add_test(protocol-parser test_protocolparser)

