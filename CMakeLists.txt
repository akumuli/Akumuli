project(Akumuli)
cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall -Werror")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror")

find_package(Boost 1.53.0
    COMPONENTS
    unit_test_framework
    program_options
    system
    thread
    filesystem
    coroutine
    context
    REQUIRED
)

find_package(Log4Cxx REQUIRED)

find_package(APR REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories("${APR_INCLUDE_DIR}")

#if(CMAKE_COMPILER_IS_GNUCXX)
add_definitions(-std=gnu++0x -fvisibility=hidden)
#endif()

include_directories(./libakumuli/include)

enable_testing()
add_subdirectory(src)
add_subdirectory(libakumuli)
