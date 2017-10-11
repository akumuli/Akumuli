add_executable(storage_test storage_test.cpp)

include_directories(../libakumuli)

target_link_libraries(storage_test
    akumuli
    "${SQLITE3_LIBRARY}"
    "${APRUTIL_LIBRARY}"
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    )

file(COPY
    akumulid_test_tools.py
    test_data_ingestion.py
    test_data_ingestion_bulk.py
    test_query_language.py
    test_continuous_queries.py
    test_volume_overflow.py
    test_database_overflow.py
    test_restart.py
    test_kill.py
    test_concurrency.py
    test_ingestion_errors.py
    test_join_query.py
    test_search_api.py
    roundtrip.sh
    DESTINATION
    ./
)

