project(Akumuli)
cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

find_package(Boost 1.53.0
    COMPONENTS
    unit_test_framework
    system
    REQUIRED
)

find_package(Log4Cxx REQUIRED)

find_package(APR REQUIRED)

include_directories(${Boost_INCLUDE_DIRS}) 
include_directories(${APR_INCLUDE_DIR})

#if(CMAKE_COMPILER_IS_GNUCXX)
add_definitions(-std=gnu++0x)
#endif()

include_directories(./include)
include_directories(./third_party)

add_subdirectory(src)
enable_testing()
add_subdirectory(unit_tests)
add_subdirectory(tests/fill_test)
add_subdirectory(tests/bench)
add_subdirectory(tool)
