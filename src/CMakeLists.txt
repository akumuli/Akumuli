add_library(akumuli SHARED
    ../include/page_manager.h
    ../include/config.h
    ../include/page.h
    ../include/akumuli.h
    page_manager.cpp
    page.cpp
    akumuli.cpp
)

target_link_libraries(akumuli ${Log4Cxx_LIBRARIES})
