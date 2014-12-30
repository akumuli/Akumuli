# Main executable
add_executable(akumulid
    main.cpp
    logger.cpp logger.h
    stream.cpp stream.h
    resp.cpp resp.h
    protocolparser.cpp protocolparser.h
    protocol_consumer.h
    ingestion_pipeline.cpp ingestion_pipeline.h
    tcp_server.cpp tcp_server.h
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

# Small utils
add_executable(test_utils
    test_utils.cpp
    expected.h
)
target_link_libraries(test_utils
    ${Boost_LIBRARIES}
)
add_test(utils test_utils)

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

# Pipeline test
add_executable(test_pipeline
    test_pipeline.cpp
    ingestion_pipeline.cpp
    logger.cpp
)
target_link_libraries(test_pipeline
    jemalloc
    akumuli
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
add_test(protocol-parser test_protocolparser)

##############################################
#                                            #
#          Performance tests                 #
#                                            #
##############################################

# RESP perf test
add_executable(perf_respstream
    perf_respstream.cpp
    stream.cpp stream.h
    resp.cpp resp.h
)
target_link_libraries(perf_respstream
    ${Boost_LIBRARIES}
)

# Pipeline perf test
add_executable(perf_pipeline
    perf_pipeline.cpp
    ingestion_pipeline.cpp
    logger.cpp
)
target_link_libraries(perf_pipeline
    jemalloc
    akumuli
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
