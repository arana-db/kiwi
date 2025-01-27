# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

# Always invoke `FIND_PACKAGE(Protobuf)` for importing function protobuf_generate_cpp

IF (${LIB_BUILD_TYPE} STREQUAL DEBUG)
    SET(LIB_PROTOBUF "libprotobufd.a")
    SET(LIB_PROTOC "libprotocd.a")
ELSE ()
    SET(LIB_PROTOBUF "libprotobuf.a")
    SET(LIB_PROTOC "libprotoc.a")
ENDIF ()

SET(PROTOBUF_SOURCES_DIR "${LIB_SOURCE_DIR}/protobuf" CACHE PATH "Path to protobuf sources")
SET(PROTOBUF_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "protobuf include directory." FORCE)
SET(PROTOBUF_LIBRARY "${LIB_INSTALL_DIR}/${LIB_PROTOBUF}" CACHE FILEPATH "protobuf install directory." FORCE)
SET(PROTOC_LIBRARY "${LIB_INSTALL_DIR}/${LIB_PROTOC}" CACHE FILEPATH "protoc install directory." FORCE)
SET(PROTOBUF_PROTOC "${LIB_INSTALL_PREFIX}/bin/protoc")

ExternalProject_Add(
        protobuf
        UPDATE_COMMAND ""
        LOG_CONFIGURE 1
        LOG_BUILD 1
        LOG_INSTALL 1
        SOURCE_SUBDIR cmake
        DEPENDS zlib
        URL "https://github.com/protocolbuffers/protobuf/archive/v3.18.0.tar.gz"
        URL_HASH SHA256=14e8042b5da37652c92ef6a2759e7d2979d295f60afd7767825e3de68c856c54
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download"
        DOWNLOAD_NAME "protobuf-3.18.0.tar.gz"
        SOURCE_DIR ${PROTOBUF_SOURCES_DIR}
        DOWNLOAD_NO_PROGRESS 1
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DBUILD_SHARED_LIBS=OFF
        -Dprotobuf_BUILD_TESTS=OFF
        -Dprotobuf_BUILD_LIBPROTOC=ON
        ${EXTERNAL_GENERATOR}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        BUILD_BYPRODUCTS ${LIB_PROTOBUF}
)
