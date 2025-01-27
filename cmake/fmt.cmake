# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.


IF (${LIB_BUILD_TYPE} STREQUAL DEBUG)
    SET(LIB_FMT libfmtd.a)
ELSE ()
    SET(LIB_FMT libfmt.a)
ENDIF ()

SET(FMT_SOURCES_DIR "${LIB_SOURCE_DIR}/fmt" CACHE PATH "Path to fmt sources")
SET(FMT_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "fmt include directory." FORCE)
SET(FMT_LIBRARIES "${LIB_INSTALL_DIR}/${LIB_FMT}" CACHE FILEPATH "fmt library directory." FORCE)

ExternalProject_Add(
        fmt
        URL https://github.com/fmtlib/fmt/archive/10.1.1.zip
        URL_HASH SHA256=3c2e73019178ad72b0614a3124f25de454b9ca3a1afe81d5447b8d3cbdb6d322
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download"
        DOWNLOAD_NAME "fmt-10.1.1.zip"
        SOURCE_DIR ${FMT_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DFMT_DOC=FALSE
        -DFMT_TEST=FALSE
        -DSHARED_LIBS=FALSE
        ${EXTERNAL_GENERATOR}
        BUILD_BYPRODUCTS ${FMT_LIBRARIES}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        UPDATE_COMMAND ""
)
