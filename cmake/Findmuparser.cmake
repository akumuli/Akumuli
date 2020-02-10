# Find the muparser library
#
# Usage:
#   find_package(muparser [REQUIRED] [QUIET] )
#
# Hints may be given by the (environment) variables
#   muparser_ROOT                ... Path to the installation library
#
# It sets the following variables:
#   muparser_FOUND               ... true if muparser is found on the system
#   muparser_LIBRARIES           ... full path to muparser library
#   muparser_INCLUDES            ... muparser include directory
#
#

find_package(PkgConfig)

pkg_check_modules(PC_MUPARSER QUIET muparser)
set(MUPARSER_DEFINITIONS ${PC_MUPARSER_CFLAGS_OTHER})

find_path(muparser_INCLUDES muParser.h
          HINTS ${PC_MUPARSER_INCLUDEDIR} ${PC_MUPARSER_INCLUDE_DIRS})

find_library(muparser_LIBRARIES NAMES muparser libmuparser
             HINTS ${PC_MUPARSER_LIBDIR} ${PC_MUPARSER_LIBRARY_DIRS} )

set(MUPARSER_LIBRARIES ${muparser_LIBRARIES} )
set(MUPARSER_INCLUDE_DIRS ${muparser_INCLUDES} )

set(muparser_FOUND FALSE)
if(muparser_INCLUDES AND muparser_LIBRARIES)
    set(muparser_FOUND TRUE)
    MESSAGE(STATUS "Found muParser: ${muparser_LIBRARIES}")
else()
    MESSAGE(STATUS "Not found muParser")
endif()

mark_as_advanced(muparser_INCLUDES muparser_LIBRARIES )

