# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(GTEST_SOURCES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download/source/extern_gtest" CACHE PATH "Path to gtest sources")
SET(GTEST_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "gtest include directory." FORCE)
SET(GTEST_LIBRARIES "${LIB_INSTALL_DIR}/libgtest.a" CACHE FILEPATH "gtest lib directory." FORCE)
SET(GTEST_MAIN_LIBRARIES "${LIB_INSTALL_DIR}/libgtest_main.a" CACHE FILEPATH "gtest main include directory." FORCE)
SET(GMOCK_LIBRARIES "${LIB_INSTALL_DIR}/libgmock.a" CACHE FILEPATH "gmock directory." FORCE)

ExternalProject_Add(
        extern_gtest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
        GIT_SHALLOW true
        SOURCE_DIR ${GTEST_SOURCES_DIR}
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
)

ADD_DEPENDENCIES(extern_gtest snappy gflags zlib)
ADD_LIBRARY(gtest STATIC IMPORTED GLOBAL)
ADD_LIBRARY(gtest_main STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARIES})
SET_PROPERTY(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARIES})
ADD_DEPENDENCIES(gtest extern_gtest)
