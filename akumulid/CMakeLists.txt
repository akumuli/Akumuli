# Main executable
add_executable(akumulid
    main.cpp
    logger.cpp
    stream.cpp
    resp.cpp
    protocolparser.cpp
    storage_api.cpp
    tcp_server.cpp
    udp_server.cpp
    httpserver.cpp
    query_results_pooler.cpp
    signal_handler.cpp
)

target_link_libraries(akumulid
    jemalloc
    akumuli
    "${SQLITE3_LIBRARY}"
    "${LOG4CXX_LIBRARIES}"
    "${APR_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    ${Boost_LIBRARIES}
    ${LIBMICROHTTPD_LIBRARY}
    pthread
)

include(CppcheckTargets)
add_cppcheck(akumulid UNUSED_FUNCTIONS STYLE POSSIBLE_ERROR FORCE)


install(
    TARGETS
        akumulid
    RUNTIME DESTINATION
        ${CMAKE_INSTALL_BINDIR}
)

set(CPACK_GENERATOR "RPM;DEB")

set(CPACK_PACKAGE_NAME "akumuli")
set(CPACK_PACKAGE_ARCHITECTURE "${APP_ARCH}")
set(CPACK_PACKAGE_MAINTAINER "Evgeny Lazin")
set(CPACK_PACKAGE_DESCRIPTION "Time-series database")

set(CPACK_PACKAGE_VERSION "${APP_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${APP_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${APP_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${APP_VERSION_PATCH}")

# RPM variables
set(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_RPM_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES} apr-util-sqlite >= 1.5.0 ")

# DEB variables
set(CPACK_DEBIAN_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${CPACK_PACKAGE_ARCHITECTURE})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${CPACK_PACKAGE_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION})
set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

set(CPACK_PACKAGE_FILE_NAME "akumuli_${APP_VERSION}-1_${CPACK_PACKAGE_ARCHITECTURE}")

# autogenerate dependency information
set (CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS} libaprutil1 (>= 1.0.5), libaprutil1-dbd-sqlite3 (>= 1.5.0) ")

include(CPack)
