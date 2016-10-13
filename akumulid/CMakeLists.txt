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
    akumuli2
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


install(
    TARGETS
        akumulid
    RUNTIME DESTINATION
        ${CMAKE_INSTALL_PREFIX}/bin
)

set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_PACKAGE_NAME "akumuli")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Evgeny Lazin")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Time-series storage engine")
set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

set(CPACK_PACKAGE_VERSION "0.4.0")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "4")
set(CPACK_PACKAGE_VERSION_PATCH "0")

include(CPack)
