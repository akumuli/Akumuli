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
# It defines the following targets:
#   muparser::muparser           ... muparser library to link against
#

find_library(muparser_LIBRARY
    NAMES muparser muparserd
    HINTS ${muparser_ROOT} ENV muparser_ROOT
)

find_path(muparser_INCLUDE_DIR
    muParserDef.h
    HINTS ${muparser_ROOT} ENV muparser_ROOT
)

mark_as_advanced(muparser_INCLUDE_DIR muparser_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(muparser
    DEFAULT_MSG
    muparser_LIBRARY muparser_INCLUDE_DIR)

if (muparser_FOUND)
    set(muparser_LIBRARIES ${muparser_LIBRARY} )
    set(muparser_INCLUDES ${muparser_INCLUDE_DIR} )

    # add the target
    add_library(muparser::muparser SHARED IMPORTED)
    set_target_properties(muparser::muparser
      PROPERTIES IMPORTED_LOCATION ${muparser_LIBRARIES}
                 INTERACE_INCLUDE_DIRECTORIES ${muparser_INCLUDES}
    )
endif()

