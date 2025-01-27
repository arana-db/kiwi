# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

IF (${LIB_BUILD_TYPE} STREQUAL DEBUG)
    SET(LIB_GFLAGS libgflags_debug.a)
ELSE ()
    SET(LIB_GFLAGS libgflags.a)
ENDIF ()

SET(GFLAGS_SOURCES_DIR "${LIB_SOURCE_DIR}/gflags" CACHE PATH "Path to gflags sources")
SET(GFLAGS_INCLUDE_DIR ${LIB_INCLUDE_DIR} CACHE PATH "gflags include directory." FORCE)
SET(GFLAGS_LIBRARIES ${LIB_INSTALL_DIR}/${LIB_GFLAGS} CACHE FILEPATH "gflags library." FORCE)
SET(GFLAGS_LIBRARY ${LIB_INSTALL_DIR}/${LIB_GFLAGS} CACHE FILEPATH "gflags library." FORCE)

ExternalProject_Add(
        gflags
        URL https://github.com/gflags/gflags/archive/v2.2.2.zip
        URL_HASH SHA256=19713a36c9f32b33df59d1c79b4958434cb005b5b47dc5400a7a4b078111d9b5
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download"
        DOWNLOAD_NAME "gflags-2.2.2.zip"
        SOURCE_DIR ${GFLAGS_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_GENERATOR}
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DGFLAGS_BUILD_STATIC_LIBS=ON
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DBUILD_SHARED_LIBS=OFF
        -DGFLAGS_BUILD_SHARED_LIBS=OFF
        -DGFLAGS_BUILD_gflags_LIB=ON
        -DGFLAGS_BUILD_gflags_nothreads_LIB=ON
        -DGFLAGS_NAMESPACE=gflags
        -DGFLAGS_BUILD_TESTING=OFF
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        UPDATE_COMMAND ""
        BUILD_BYPRODUCTS ${GFLAGS_LIBRARY}
)
