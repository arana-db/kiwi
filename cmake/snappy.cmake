# Copyright (c) 2024-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(Snappy_SOURCES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download/source/extern_snappy" CACHE PATH "Path to snappy sources")
SET(Snappy_INCLUDE_DIRS "${LIB_INCLUDE_DIR}" CACHE PATH "Snappy include directory." FORCE)
SET(Snappy_LIBRARIES "${LIB_INSTALL_DIR}/libsnappy.a" CACHE FILEPATH "Snappy install directory." FORCE)

ExternalProject_Add(
        extern_snappy
        ${EXTERNAL_PROJECT_LOG_ARGS}
        GIT_REPOSITORY "https://github.com/google/snappy.git"
        GIT_TAG "1.2.1"
        GIT_SHALLOW true
        SOURCE_DIR ${Snappy_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DSNAPPY_BUILD_TESTS=OFF
        -DSNAPPY_BUILD_BENCHMARKS=OFF
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=OFF
)

ADD_LIBRARY(snappy STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION ${Snappy_LIBRARIES})
ADD_DEPENDENCIES(snappy extern_snappy)
