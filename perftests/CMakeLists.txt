##############################################
#                                            #
#          Performance tests                 #
#                                            #
##############################################

include_directories(../akumulid)
include_directories(../libakumuli)

# RESP perf test
add_executable(
    perf_respstream
    perf_respstream.cpp
    perftest_tools.cpp
    ../akumulid/stream.cpp 
    ../akumulid/stream.h 
    ../akumulid/resp.cpp 
    ../akumulid/resp.h
)
target_link_libraries(perf_respstream
    ${Boost_LIBRARIES}
)

# Pipeline perf test
add_executable(
    perf_pipeline
    perf_pipeline.cpp
    perftest_tools.cpp
    ../akumulid/ingestion_pipeline.cpp
    ../akumulid/logger.cpp
)
target_link_libraries(perf_pipeline
    jemalloc
    akumuli
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)

# TCP server perf test
add_executable(
    perf_tcp_server
    perf_tcp_server.cpp
    perftest_tools.cpp
    ../akumulid/tcp_server.cpp
    ../akumulid/resp.cpp
    ../akumulid/protocolparser.cpp
    ../akumulid/stream.cpp
    ../akumulid/ingestion_pipeline.cpp
    ../akumulid/logger.cpp
)
target_link_libraries(perf_tcp_server
    jemalloc
    akumuli
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)





#########################################
#                                       #
#          libakumuli perftests         #
#                                       #
#########################################

# Series parser perftest
add_executable(
    perf_seriesmatcher
    perf_seriesmatcher.cpp
    ../libakumuli/seriesparser.cpp
    ../libakumuli/stringpool.cpp
    ../libakumuli/queryprocessor.cpp
    ../libakumuli/util.cpp
)

target_link_libraries(
    perf_seriesmatcher
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)

