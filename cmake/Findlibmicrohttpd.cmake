# - Try to find libmicrohttpd
# Once done this will define
#
#  MICROHTTPD_FOUND - system has libmicrohttpd
#  MICROHTTPD_INCLUDE_DIRS - the libmicrohttpd include directory
#  MICROHTTPD_LIBRARIES - Link these to use libmicrohttpd
#  MICROHTTPD_DEFINITIONS - Compiler switches required for using libmicrohttpd
#
#  Copyright (c) 2011 Wesley Moore <wes@wezm.net>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#


if (LIBMICROHTTPD_LIBRARIES AND LIBMICROHTTPD_INCLUDE_DIRS)
  # in cache already
  set(LIBMICROHTTPD_FOUND TRUE)
else (LIBMICROHTTPD_LIBRARIES AND LIBMICROHTTPD_INCLUDE_DIRS)
  # use pkg-config to get the directories and then use these values
  # in the FIND_PATH() and FIND_LIBRARY() calls
  if (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    include(UsePkgConfig)
    pkgconfig(libmicrohttpd _LIBMICROHTTPD_INCLUDEDIR _LIBMICROHTTPD_LIBDIR _LIBMICROHTTPD_LDFLAGS _LIBMICROHTTPD_CFLAGS)
  else (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    find_package(PkgConfig)
    if (PKG_CONFIG_FOUND)
      pkg_check_modules(_LIBMICROHTTPD libmicrohttpd)
    endif (PKG_CONFIG_FOUND)
  endif (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
  find_path(LIBMICROHTTPD_INCLUDE_DIR
    NAMES
      microhttpd.h
    PATHS
      ${_LIBMICROHTTPD_INCLUDEDIR}
      /usr/include
      /usr/local/include
      /usr/pkg/include
      /opt/local/include
      /sw/include
  )

  find_library(LIBMICROHTTPD_LIBRARY
    NAMES
      microhttpd
    PATHS
      ${_LIBMICROHTTPD_LIBDIR}
      /usr/lib
      /usr/local/lib
      /usr/pkg/lib
      /opt/local/lib
      /sw/lib
  )

  if (LIBMICROHTTPD_LIBRARY)
    set(LIBMICROHTTPD_FOUND TRUE)
  endif (LIBMICROHTTPD_LIBRARY)

  set(LIBMICROHTTPD_INCLUDE_DIRS
    ${LIBMICROHTTPD_INCLUDE_DIR}
  )

  if (LIBMICROHTTPD_FOUND)
    set(LIBMICROHTTPD_LIBRARIES
      ${LIBMICROHTTPD_LIBRARIES}
      ${LIBMICROHTTPD_LIBRARY}
    )
  endif (LIBMICROHTTPD_FOUND)

  if (LIBMICROHTTPD_INCLUDE_DIRS AND LIBMICROHTTPD_LIBRARIES)
     set(LIBMICROHTTPD_FOUND TRUE)
  endif (LIBMICROHTTPD_INCLUDE_DIRS AND LIBMICROHTTPD_LIBRARIES)

  if (LIBMICROHTTPD_FOUND)
    if (NOT LIBMICROHTTPD_FIND_QUIETLY)
      message(STATUS "Found libmicrohttpd: ${LIBMICROHTTPD_LIBRARIES}")
    endif (NOT LIBMICROHTTPD_FIND_QUIETLY)
  else (LIBMICROHTTPD_FOUND)
    if (LIBMICROHTTPD_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find libmicrohttpd")
    endif (LIBMICROHTTPD_FIND_REQUIRED)
  endif (LIBMICROHTTPD_FOUND)

  # show the LIBMICROHTTPD_INCLUDE_DIRS and LIBMICROHTTPD_LIBRARIES variables only in the advanced view
  mark_as_advanced(LIBMICROHTTPD_INCLUDE_DIRS LIBMICROHTTPD_LIBRARIES)

endif (LIBMICROHTTPD_LIBRARIES AND LIBMICROHTTPD_INCLUDE_DIRS)


