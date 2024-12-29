# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(LZ4_SOURCES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download/source/extern_lz4" CACHE PATH "Path to lz4 sources")
SET(LZ4_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "lz4 include directory." FORCE)
SET(LZ4_LIBRARIES "${LIB_INSTALL_DIR}/liblz4.a" CACHE FILEPATH "lz4 include directory." FORCE)

ExternalProject_Add(
        extern_lz4
        URL https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz
        URL_HASH SHA256=0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download"
        DOWNLOAD_NAME "lz4-1.9.4.tar.gz"
        SOURCE_DIR ${LZ4_SOURCES_DIR}
        DOWNLOAD_NO_PROGRESS 1
        ${EXTERNAL_PROJECT_LOG_ARGS}
        SOURCE_SUBDIR build/cmake
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DBUILD_TESTING=OFF
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=OFF
        BUILD_COMMAND make -j${CPU_CORE}
)

ADD_LIBRARY(lz4 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET lz4 PROPERTY IMPORTED_LOCATION ${LZ4_LIBRARIES})
ADD_DEPENDENCIES(lz4 extern_lz4)
