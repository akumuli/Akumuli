add_library(akumuli SHARED
    ../include/storage_manager.h
    ../include/page_manager.h
    ../include/config.h
    ../include/page.h
    ../include/akumuli.h
    ../include/util.h
    storage_manager.cpp
    page_manager.cpp
    page.cpp
    akumuli.cpp
    util.cpp
)
