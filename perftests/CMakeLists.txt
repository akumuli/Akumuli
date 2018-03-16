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
set_target_properties(perf_respstream PROPERTIES EXCLUDE_FROM_ALL 1)

# Pipeline perf test
add_executable(
    perf_pipeline
    perf_pipeline.cpp
    perftest_tools.cpp
    ../akumulid/ingestion_pipeline.cpp
    ../akumulid/logger.cpp
)
target_link_libraries(perf_pipeline
    akumuli
    "${JEMALLOC_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_pipeline PROPERTIES EXCLUDE_FROM_ALL 1)

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
    akumuli
    "${JEMALLOC_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_tcp_server PROPERTIES EXCLUDE_FROM_ALL 1)



#########################################
#                                       #
#          libakumuli perftests         #
#                                       #
#########################################

# Series parser perftest
add_executable(
    perf_seriesmatcher
    perf_seriesmatcher.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
    ../libakumuli/queryprocessor.cpp
    ../libakumuli/saxencoder.cpp
    ../libakumuli/anomalydetector.cpp
    ../libakumuli/hashfnfamily.cpp
    ../libakumuli/util.cpp
    ../libakumuli/datetime.cpp
)

target_link_libraries(
    perf_seriesmatcher
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)
set_target_properties(perf_seriesmatcher PROPERTIES EXCLUDE_FROM_ALL 1)

# Datetime parser perftest
add_executable(
    perf_datetime_parsing
    perf_datetime_parsing.cpp
    perftest_tools.cpp
    ../libakumuli/datetime.cpp
)

target_link_libraries(
    perf_datetime_parsing
    "${JEMALLOC_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_datetime_parsing PROPERTIES EXCLUDE_FROM_ALL 1)

# Compression perftest
add_executable(
    perf_compression
    perf_compression.cpp
    perftest_tools.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/column_store.cpp
    ../libakumuli/storage_engine/nbtree.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/util.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/datetime.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
)

target_link_libraries(
    perf_compression
    z
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_compression PROPERTIES EXCLUDE_FROM_ALL 1)

# Compression throughput perftest
add_executable(
    perf_compression_throughput
    perf_compression_throughput.cpp
    perftest_tools.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
)

target_link_libraries(
    perf_compression_throughput
    z
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_compression_throughput PROPERTIES EXCLUDE_FROM_ALL 1)

# Ingestion perftest
add_executable(perf_ingestion perf_ingestion.cpp)

target_link_libraries(perf_ingestion
    akumuli
    "${JEMALLOC_LIBRARY}"
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_ingestion PROPERTIES EXCLUDE_FROM_ALL 1)

# Parallel ingestion perftest
add_executable(perf_parallel_ingestion perf_parallel_ingestion.cpp)

target_link_libraries(perf_parallel_ingestion
    akumuli
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_parallel_ingestion PROPERTIES EXCLUDE_FROM_ALL 1)


# Inverted index perftest
add_executable(
    perf_invertedindex
    perf_invertedindex.cpp
    perftest_tools.cpp
    ../libakumuli/index/invertedindex.cpp
)

target_link_libraries(
    perf_invertedindex
    "${JEMALLOC_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_invertedindex PROPERTIES EXCLUDE_FROM_ALL 1)

# Blockstore perftest
add_executable(
    perf_blockstore
    perf_blockstore.cpp
    perftest_tools.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/status_util.cpp
)

target_link_libraries(
    perf_blockstore
    "${JEMALLOC_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_blockstore PROPERTIES EXCLUDE_FROM_ALL 1)

# NBTree perftest
add_executable(
    perf_nbtree
    perf_nbtree.cpp
    perftest_tools.cpp
    ../libakumuli/util.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/storage_engine/nbtree.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/status_util.cpp
)

target_link_libraries(
    perf_nbtree
    "${JEMALLOC_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
)
set_target_properties(perf_nbtree PROPERTIES EXCLUDE_FROM_ALL 1)
