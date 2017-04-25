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
)

include(CppcheckTargets)
add_cppcheck(akumulid UNUSED_FUNCTIONS STYLE POSSIBLE_ERROR FORCE)


install(
    TARGETS
        akumulid
    RUNTIME DESTINATION
        #${CMAKE_INSTALL_PREFIX}/bin
        /usr/bin
)

set(CPACK_GENERATOR "DEB")
set(CPACK_DEBIAN_PACKAGE_NAME "akumuli")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Evgeny Lazin")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Time-series storage engine")
set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

set(CPACK_PACKAGE_VERSION "${APP_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${APP_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${APP_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${APP_VERSION_PATCH}")


set(CPACK_PACKAGE_FILE_NAME
"akumuli_${APP_VERSION}-${PACKAGE_VERSION}_${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}")

include(CPack)
