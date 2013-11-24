add_library(akumuli SHARED
    ../include/page_manager.h
    ../include/config.h
    ../include/page.h
    ../include/akumuli.h
    ../include/util.h
    page_manager.cpp
    page.cpp
    akumuli.cpp
    util.cpp
)
