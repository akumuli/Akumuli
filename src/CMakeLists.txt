add_library(spatium SHARED 
    ../include/buffer_manager.h
    ../include/config.h
    ../include/page.h
    ../include/recorder.h
    buffer_manager.cpp 
    page.cpp
    recorder.cpp
)

target_link_libraries(spatium ${Log4Cxx_LIBRARIES})
