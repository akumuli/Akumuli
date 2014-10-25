add_executable(ingestion_test main.cpp)
target_link_libraries(ingestion_test
    akumuli
    "${APR_LIBRARY}"
    ${Boost_LIBRARIES}
    libboost_coroutine.a
    libboost_context.a
)
