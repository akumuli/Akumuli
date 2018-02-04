######################
#     Test suite     #
######################

include_directories(../akumulid)
include_directories(../libakumuli)

# Streams
add_executable(
    test_streams
    test_stream.cpp
    ../akumulid/stream.cpp 
    ../akumulid/stream.h
)
target_link_libraries(test_streams
    ${Boost_LIBRARIES}
    pthread
)
add_test(streams test_streams)


# RESP
add_executable(
    test_respstream
    test_respstream.cpp
    ../akumulid/stream.cpp 
    ../akumulid/stream.h
    ../akumulid/resp.cpp 
    ../akumulid/resp.h
)
target_link_libraries(test_respstream
    ${Boost_LIBRARIES}
    pthread
)
add_test(respstream test_respstream)


# Protocol parser
add_executable(
    test_protocolparser
    test_protocolparser.cpp
    ../akumulid/protocolparser.cpp 
    ../akumulid/protocolparser.h
    ../akumulid/logger.cpp 
    ../akumulid/logger.h
    ../akumulid/stream.cpp 
    ../akumulid/stream.h
    ../akumulid/resp.cpp 
    ../akumulid/resp.h
)
target_link_libraries(
    test_protocolparser
    akumuli
    sqlite3
    ${Boost_LIBRARIES}
    "${LOG4CXX_LIBRARIES}"
    pthread
)
add_test(protocol-parser test_protocolparser)


# TCPServer test
add_executable(
    test_tcp_server
    test_tcp_server.cpp
    ../akumulid/ingestion_pipeline.cpp
    ../akumulid/tcp_server.cpp
    ../akumulid/signal_handler.cpp
    ../akumulid/resp.cpp
    ../akumulid/stream.cpp
    ../akumulid/protocolparser.cpp
    ../akumulid/logger.cpp
)
target_link_libraries(test_tcp_server
    akumuli
    "${JEMALLOC_LIBRARY}"
    "${SQLITE3_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)
add_test(tcp-server test_tcp_server)

# QueryCursor

# Pipeline test
add_executable(
    test_querycursor
    test_querycursor.cpp
    ../akumulid/query_results_pooler.cpp
    ../akumulid/ingestion_pipeline.cpp
    ../akumulid/logger.cpp
)
target_link_libraries(
    test_querycursor
    akumuli
    "${JEMALLOC_LIBRARY}"
    "${SQLITE3_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)
add_test(querycursor test_querycursor)


##########################################
#                                        #
#           libakumuli tests             #
#                                        #
##########################################

# Cursor tests
add_executable(
    test_cursor
    test_cursor.cpp
    ../libakumuli/cursor.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/storage_engine/compression.cpp
)

target_link_libraries(
    test_cursor
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(cursor test_cursor)

# Mmap test
add_executable(
    test_util
    test_util.cpp
    ../libakumuli/util.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/log_iface.cpp
)

target_link_libraries(
    test_util
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(util test_util)

# Compression test
add_executable(
    test_compression
    test_compression.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
)

target_link_libraries(
    test_compression
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(compression test_compression)

# Storage test
add_executable(
    test_storage
    test_storage.cpp
    ../libakumuli/storage2.cpp
    ../libakumuli/metadatastorage.cpp
    ../libakumuli/util.cpp
    ../libakumuli/datetime.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
    ../libakumuli/index/invertedindex.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/nbtree.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/storage_engine/operators/operator.cpp
    ../libakumuli/storage_engine/operators/aggregate.cpp
    ../libakumuli/storage_engine/operators/scan.cpp
    ../libakumuli/storage_engine/operators/join.cpp
    ../libakumuli/storage_engine/operators/merge.cpp
    ../libakumuli/storage_engine/column_store.cpp
    ../libakumuli/query_processing/queryparser.cpp
    ../libakumuli/query_processing/queryplan.cpp
    # query processor
    ../libakumuli/queryprocessor.cpp
    ../libakumuli/queryprocessor_framework.cpp
    ../libakumuli/saxencoder.cpp
    #../libakumuli/anomalydetector.cpp
    #../libakumuli/hashfnfamily.cpp
    #../libakumuli/query_processing/anomaly.cpp
    ../libakumuli/query_processing/sax.cpp
    ../libakumuli/query_processing/rate.cpp
    ../libakumuli/query_processing/sliding_window.cpp
    #../libakumuli/query_processing/filterbyid.cpp
    ../libakumuli/query_processing/limiter.cpp

)

target_link_libraries(
    test_storage
    sqlite3
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(storage test_storage)

# Series parser tests
add_executable(
    test_seriesparser
    test_parser.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
    ../libakumuli/index/invertedindex.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/datetime.cpp
)

target_link_libraries(
    test_seriesparser
    pthread
    ${Boost_LIBRARIES}
    "${APR_LIBRARY}"
)

add_test(seriesparser test_seriesparser)

# Datetime test
add_executable(
    test_datetime
    test_datetime.cpp
    ../libakumuli/datetime.cpp
)

target_link_libraries(
    test_datetime
    ${Boost_LIBRARIES}
    pthread
)

add_test(datetime test_datetime)

# Sax test
add_executable(
    test_sax
    test_sax.cpp
    ../libakumuli/saxencoder.cpp
)

target_link_libraries(
    test_sax
    ${Boost_LIBRARIES}
    pthread
)

add_test(SAX test_sax)


# Blockstore test
add_executable(
    test_blockstore
    test_blockstore.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/util.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/status_util.cpp
)

target_link_libraries(
    test_blockstore
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(blockstore test_blockstore)

# NB+tree test
add_executable(
    test_nbtree
    test_nbtree.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/nbtree.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/storage_engine/operators/operator.cpp
    ../libakumuli/storage_engine/operators/aggregate.cpp
    ../libakumuli/storage_engine/operators/scan.cpp
    ../libakumuli/storage_engine/operators/join.cpp
    ../libakumuli/storage_engine/operators/merge.cpp
    ../libakumuli/util.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/crc32c.cpp
)

target_compile_definitions(test_nbtree PRIVATE AKU_UNIT_TEST_CONTEXT=1)

target_link_libraries(
    test_nbtree
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    "${SQLITE3_LIBRARY}"
    pthread
)

add_test(nbtree test_nbtree)

# Column-store test
add_executable(
    test_column_store
    test_column_store.cpp
    ../libakumuli/storage_engine/blockstore.cpp
    ../libakumuli/storage_engine/volume.cpp
    ../libakumuli/storage_engine/nbtree.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/storage_engine/operators/operator.cpp
    ../libakumuli/storage_engine/operators/aggregate.cpp
    ../libakumuli/storage_engine/operators/scan.cpp
    ../libakumuli/storage_engine/operators/join.cpp
    ../libakumuli/storage_engine/operators/merge.cpp
    ../libakumuli/storage_engine/column_store.cpp
    ../libakumuli/query_processing/queryplan.cpp
    ../libakumuli/util.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/log_iface.cpp
    ../libakumuli/crc32c.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
    ../libakumuli/index/invertedindex.cpp
    ../libakumuli/metadatastorage.cpp
)

target_compile_definitions(test_column_store PRIVATE AKU_UNIT_TEST_CONTEXT=1)

target_link_libraries(
    test_column_store
    sqlite3
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    pthread
)

add_test(column_store test_column_store)

