# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(ZLIB_SOURCES_DIR "${LIB_SOURCE_DIR}/zlib" CACHE PATH "Path to zlib sources")
SET(ZLIB_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "zlib include directory." FORCE)
SET(ZLIB_LIBRARIES "${LIB_INSTALL_DIR}/libz.a" CACHE FILEPATH "zlib library." FORCE)

ExternalProject_Add(
        zlib
        ${EXTERNAL_PROJECT_LOG_ARGS}
        GIT_REPOSITORY  "https://github.com/madler/zlib.git"
        GIT_TAG         "v1.2.8"
        GIT_SHALLOW     true
        SOURCE_DIR      ${ZLIB_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        ${EXTERNAL_GENERATOR}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        UPDATE_COMMAND ""
        BUILD_BYPRODUCTS ${ZLIB_LIBRARIES}
)
