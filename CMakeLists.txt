project(Recorder)
cmake_minimum_required(VERSION 2.8)

if(CMAKE_COMPILER_IS_GNUCXX)
    add_definitions(-std=gnu++0x)
endif()

include_directories(./include)

add_subdirectory(src)
add_subdirectory(tests)
