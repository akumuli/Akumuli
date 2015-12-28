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

file(COPY
    akumulid_test_tools.py
    test_data_ingestion.py
    test_query_language.py
    test_continuous_queries.py
    test_volume_overflow.py
    test_restart.py
    DESTINATION
    ./
)
