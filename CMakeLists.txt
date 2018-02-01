project(Akumuli)
cmake_minimum_required(VERSION 2.8)

set(PACKAGE_VERSION "1ubuntu1.0")
set(APP_VERSION_MAJOR "0")
set(APP_VERSION_MINOR "7")
set(APP_VERSION_PATCH "37")

set(APP_VERSION "${APP_VERSION_MAJOR}.${APP_VERSION_MINOR}.${APP_VERSION_PATCH}")
add_definitions(-DAKU_VERSION="${APP_VERSION}")

set(CMAKE_MACOSX_RPATH 1)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

if (!APPLE)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -static-libasan")
else(!APPLE)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-pessimizing-move")
add_compile_options(-Wno-delete-non-virtual-dtor)
endif(!APPLE)

set(CMAKE_CXX_FLAGS_DEBUG 
    "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall -Werror -ggdb -fsanitize=address")

set(CMAKE_CXX_FLAGS_RELEASEGEN
    "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -mtune=generic -O3")

set(CMAKE_CXX_FLAGS_RELEASE 
    "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -march=corei7-avx -O3")

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.54.0
    COMPONENTS
    unit_test_framework
    program_options
    system
    thread
    filesystem
    regex
    date_time
    REQUIRED
)

find_package(Log4Cxx REQUIRED)
find_package(Sqlite3 REQUIRED)
find_package(APR REQUIRED)
find_package(Threads REQUIRED)
find_package(JeMalloc REQUIRED)
find_package(libmicrohttpd REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories("${APR_INCLUDE_DIR}")
include_directories("${APRUTIL_INCLUDE_DIR}")
include_directories("${SQLITE3_INCLUDE_DIR}")
include_directories("${LIBMICROHTTPD_INCLUDE_DIRS}")

add_definitions(-std=c++1y -fvisibility=hidden)

# json parser from boost::property_tree needs this badly
add_definitions(-DBOOST_SPIRIT_THREADSAFE)
add_definitions(-DBOOST_PHOENIX_THREADSAFE)

include(GNUInstallDirs)

include_directories(./include)

add_subdirectory(libakumuli)
add_subdirectory(akumulid)
add_subdirectory(perftests)
add_subdirectory(functests)
add_subdirectory(fuzzers)

enable_testing()
add_subdirectory(unittests)

file(COPY
    docker/Dockerfile
    docker/akumulid  
    docker/prev.sh
    docker/startup.sh
    DESTINATION
    ./docker
)
