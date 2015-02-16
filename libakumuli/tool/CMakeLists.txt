add_executable(akumuli-tool main.cpp)

target_link_libraries(akumuli-tool
    akumuli
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
