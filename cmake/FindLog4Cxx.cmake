# Strigi
# This notice was added by Olivier Coupelon based on the project information from http://sourceforge.net/projects/strigi
# In case of error please contact me at olivier.coupelon@teardrop.fr
# Copyright (C) 2006 Strigi

# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of the
# License, or (at your option) any later version.

# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


# - Try to find the liblog4cxx libraries
# Once done this will define
#
# Log4cxx_FOUND - system has liblog4cxx
# LOG4CXX_INCLUDE_DIR - the liblog4cxx include directory
# LOG4CXX_LIBRARIES - liblog4cxx library

FIND_PATH(LOG4CXX_INCLUDE_DIR logger.h PATHS /include/log4cxx /usr/include/log4cxx /usr/local/include/log4cxx )
FIND_LIBRARY(LOG4CXX_LIBRARIES NAMES log4cxx log4cxxd PATHS /lib /usr/lib /usr/local/lib )

IF(LOG4CXX_INCLUDE_DIR AND LOG4CXX_LIBRARIES)
  SET(Log4cxx_FOUND 1)
  #remove last /log4cxx string
  STRING(REGEX REPLACE "/log4cxx" "" LOG4CXX_INCLUDE_DIR_SUP_LEVEL ${LOG4CXX_INCLUDE_DIR})
  SET (LOG4CXX_INCLUDE_DIR ${LOG4CXX_INCLUDE_DIR_SUP_LEVEL} ${LOG4CXX_INCLUDE_DIR} )
  if(NOT Log4cxx_FIND_QUIETLY)
   message(STATUS "Found log4cxx: ${LOG4CXX_LIBRARIES}")
  endif(NOT Log4cxx_FIND_QUIETLY)
ELSE(LOG4CXX_INCLUDE_DIR AND LOG4CXX_LIBRARIES)
  SET(Log4cxx_FOUND 0 CACHE BOOL "Not found log4cxx library")
  message(STATUS "NOT Found log4cxx, disabling it")
ENDIF(LOG4CXX_INCLUDE_DIR AND LOG4CXX_LIBRARIES)

MARK_AS_ADVANCED(LOG4CXX_INCLUDE_DIR LOG4CXX_LIBRARIES)

