add_executable(storage_test storage_test.cpp)
target_link_libraries(storage_test
    akumuli
    sqlite3
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a)
