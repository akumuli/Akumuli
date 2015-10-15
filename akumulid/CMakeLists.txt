# Main executable
add_executable(akumulid
    main.cpp
    logger.cpp
    stream.cpp
    resp.cpp
    protocolparser.cpp
    ingestion_pipeline.cpp
    tcp_server.cpp
    udp_server.cpp
    httpserver.cpp
    query_results_pooler.cpp
    signal_handler.cpp
)

target_link_libraries(akumulid
    akumuli
    sqlite3
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    microhttpd
    libboost_coroutine.a
    libboost_context.a
)

include(CppcheckTargets)
add_cppcheck(akumulid UNUSED_FUNCTIONS STYLE POSSIBLE_ERROR FORCE)
