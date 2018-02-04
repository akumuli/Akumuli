include_directories(../libakumuli)
include_directories(../akumulid)

# Compression

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

# Series name parser

add_executable(afl_series_name_parser
    afl_series_name_parser.cpp
    ../libakumuli/index/seriesparser.cpp
    ../libakumuli/index/stringpool.cpp
    ../libakumuli/index/invertedindex.cpp
    ../libakumuli/util.cpp
    ../libakumuli/status_util.cpp
    ../libakumuli/log_iface.cpp
    )

target_link_libraries(afl_series_name_parser
    pthread
    "${Boost_LIBRARIES}"
    "${APR_LIBRARY}"
)

set_target_properties(afl_series_name_parser PROPERTIES EXCLUDE_FROM_ALL 1)

# Timestamp parser

add_executable(afl_timestamp_parser
    afl_timestamp_parser.cpp
    ../libakumuli/datetime.cpp
    )

target_link_libraries(afl_timestamp_parser
    pthread
    "${Boost_LIBRARIES}"
)

set_target_properties(afl_timestamp_parser PROPERTIES EXCLUDE_FROM_ALL 1)

# RESP parser

add_executable(afl_resp_parser
    afl_resp_parser.cpp
    ../akumulid/stream.cpp
    ../akumulid/resp.cpp
    )

target_link_libraries(afl_resp_parser
    pthread
    "${Boost_LIBRARIES}"
)

set_target_properties(afl_resp_parser PROPERTIES EXCLUDE_FROM_ALL 1)
