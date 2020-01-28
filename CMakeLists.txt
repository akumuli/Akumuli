project(Akumuli)
cmake_minimum_required(VERSION 2.8)

set(APP_VERSION_MAJOR "0")
set(APP_VERSION_MINOR "8")
set(APP_VERSION_PATCH "74")

set(APP_VERSION "${APP_VERSION_MAJOR}.${APP_VERSION_MINOR}.${APP_VERSION_PATCH}")
add_definitions(-DAKU_VERSION="${APP_VERSION}")
set(CMAKE_VERBOSE_MAKEFILE ON)

message("Processor: ${CMAKE_SYSTEM_PROCESSOR}")
set(FORCE_ARCH $ENV{FORCE_ARCH})
message("Force arch: ${FORCE_ARCH}")
if(FORCE_ARCH)
    set(APP_ARCH "${FORCE_ARCH}")
else()
    set(APP_ARCH "")
    if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i386")
        set(APP_ARCH "i386")
    endif()
    if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i686")
        set(APP_ARCH "i386")
    endif()
    if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
        set(APP_ARCH "amd64")
    endif()
    if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "armv7l")
        set(APP_ARCH "arm")
    endif()
    if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
        set(APP_ARCH "arm64")
    endif()
    if("${APP_ARCH}" STREQUAL "")
        message(FATAL_ERROR  "Architecture not supported")
    endif()
endif()
message("App architecture: ${APP_ARCH}")

set(CMAKE_MACOSX_RPATH 1)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

if (APPLE)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-delete-non-virtual-dtor -Wno-deprecated -Wno-pessimizing-move")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wno-delete-non-virtual-dtor -Wno-deprecated -Wno-pessimizing-move")
endif(APPLE)

# Debug flags
set(CMAKE_CXX_FLAGS_DEBUG
 "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall -Werror -ggdb -fsanitize=address -std=c++14")
set(CMAKE_C_FLAGS_DEBUG
 "${CMAKE_C_FLAGS_DEBUG} -DDEBUG -Wall -Werror -ggdb -fsanitize=address -std=c99")

# Generic release flags
set(CMAKE_CXX_FLAGS_RELEASEGEN
 "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -mtune=generic -O3 -std=c++14")
set(CMAKE_C_FLAGS_RELEASEGEN
 "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -mtune=generic -O3 -std=c99")

# Release flags
if("${APP_ARCH}" STREQUAL "amd64")
    # Use this flags only on amd64
    set(CMAKE_CXX_FLAGS_RELEASE 
     "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -march=corei7-avx -O3 -std=c++14")
    set(CMAKE_C_FLAGS_RELEASE
     "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -march=corei7-avx -O3 -std=c99")
    message("amd64 CXXFLAGS ${CMAKE_CXX_FLAGS_RELEASE}")
elseif("${APP_ARCH}" STREQUAL "i386")
    # Use this flags only on amd64
    set(CMAKE_CXX_FLAGS_RELEASE 
     "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -march=corei7-avx -O3 -std=c++14")
    set(CMAKE_C_FLAGS_RELEASE
     "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -march=corei7-avx -O3 -std=c99")
    add_definitions(-DDISABLE_X64)
    add_definitions(-DDISABLEAVX)
    add_definitions(-DDISABLE_EMBEDDED_ASM)
    message("i386 CXXFLAGS ${CMAKE_CXX_FLAGS_RELEASE}")
elseif("${APP_ARCH}" STREQUAL "arm64")
    # This work on arm
    set(CMAKE_CXX_FLAGS_RELEASE
     "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c++14")
    set(CMAKE_C_FLAGS_RELEASE
     "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c99")
    add_definitions(-DDISABLE_EMBEDDED_ASM)
    message("arm64 CXXFLAGS ${CMAKE_CXX_FLAGS_RELEASE}")
elseif("${APP_ARCH}" STREQUAL "arm")
    # This work on arm
    set(CMAKE_CXX_FLAGS_RELEASE
     "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c++14")
    set(CMAKE_C_FLAGS_RELEASE
     "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c99")
    add_definitions(-DDISABLE_EMBEDDED_ASM)
    message("arm CXXFLAGS ${CMAKE_CXX_FLAGS_RELEASE}")
else()
    # This work on arm64
    set(CMAKE_CXX_FLAGS_RELEASE
     "${CMAKE_CXX_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c++14")
    set(CMAKE_C_FLAGS_RELEASE
     "${CMAKE_C_FLAGS_RELEASE} -Wall -Werror -ffast-math -funroll-loops -O3 -std=c99")
    message("unknown CXXFLAGS ${CMAKE_CXX_FLAGS_RELEASE}")
endif()

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.50.0
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
# this doesn't work with centos, disabled until solution will be found
#find_package(JeMalloc REQUIRED)
find_package(libmicrohttpd REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories("${APR_INCLUDE_DIR}")
include_directories("${APRUTIL_INCLUDE_DIR}")
include_directories("${SQLITE3_INCLUDE_DIR}")
include_directories("${LIBMICROHTTPD_INCLUDE_DIRS}")

add_definitions(-fvisibility=hidden)

# json parser from boost::property_tree needs this badly
add_definitions(-DBOOST_SPIRIT_THREADSAFE)
add_definitions(-DBOOST_PHOENIX_THREADSAFE)
add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)

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
