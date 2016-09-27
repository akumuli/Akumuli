include_directories(../libakumuli)

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
