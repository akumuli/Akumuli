add_executable(storage_test storage_test.cpp)

include_directories(../libakumuli)

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
    test_kill.py
    test_concurrency.py
    test_ingestion_errors.py
    DESTINATION
    ./
)

add_executable(afl_compression
    afl_compression.cpp
    ../libakumuli/storage_engine/compression.cpp
    ../libakumuli/util.cpp
    ../libakumuli/log_iface.cpp
    )

target_link_libraries(afl_compression
    pthread
    "${APR_LIBRARY}"
)

set_target_properties(afl_compression PROPERTIES EXCLUDE_FROM_ALL 1)
