add_library(akumuli SHARED
    ../include/storage_manager.h
    ../include/storage.h
    ../include/config.h
    ../include/page.h
    ../include/akumuli.h
    ../include/util.h
    storage_manager.cpp
    storage.cpp
    page.cpp
    akumuli.cpp
    util.cpp
)
