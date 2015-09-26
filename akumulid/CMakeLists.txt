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
    udp_server.cpp udp_server.h
    httpserver.cpp httpserver.h
    query_results_pooler.cpp query_results_pooler.h
)

target_link_libraries(akumulid
    akumuli
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
