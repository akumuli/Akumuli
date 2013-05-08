add_library(recorder SHARED 
    ../include/buffer_manager.h
    ../include/config.h
    ../include/page.h
    ../include/recorder.h
    buffer_manager.cpp 
    page.cpp
    recorder.cpp
)
