project(Spatium)
cmake_minimum_required(VERSION 2.8)

include(cmake/FindLog4Cxx.cmake)

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 

find_package(Boost "1.49.0" 
    COMPONENTS
    unit_test_framework
    REQUIRED
)

include_directories(${Boost_INCLUDE_DIRS}) 



if(CMAKE_COMPILER_IS_GNUCXX)
    add_definitions(-std=gnu++0x)
endif()

include_directories(./include)

add_subdirectory(src)
enable_testing()
add_subdirectory(unit_tests)
add_subdirectory(tests/fill_test)
