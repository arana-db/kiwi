# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(BRPC_SOURCES_DIR "${LIB_SOURCE_DIR}/brpc" CACHE PATH "Path to brpc sources")
SET(BRPC_INSTALL_DIR ${LIB_INSTALL_PREFIX})
SET(BRPC_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "brpc include directory." FORCE)
SET(BRPC_LIBRARIES "${LIB_INSTALL_DIR}/libbrpc.a" CACHE FILEPATH "brpc library." FORCE)

SET(CMAKE_FIND_LIBRARY_SUFFIXES ${LIB_INSTALL_PREFIX})

# Reference https://stackoverflow.com/questions/45414507/pass-a-list-of-prefix-paths-to-externalproject-add-in-cmake-args
SET(CMAKE_CPP_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
# If minimal .a is need, you can set  WITH_DEBUG_SYMBOLS=OFF
ExternalProject_Add(
        brpc
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS ssl crypto zlib protobuf leveldb gflags
        URL https://github.com/apache/brpc/archive/refs/tags/1.8.0.tar.gz
        URL_HASH SHA256=13ffb2f1f57c679379a20367c744b3e597614a793ec036cd7580aae90798019d
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/download"
        DOWNLOAD_NAME "brpc-1.8.0.tar.gz"
        SOURCE_DIR ${BRPC_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DCMAKE_CPP_FLAGS=${CMAKE_CPP_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${BRPC_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_FIND_LIBRARY_SUFFIXES=${LIB_INSTALL_PREFIX}
        -DCMAKE_LIBRARY_PATH=${LIB_INSTALL_PREFIX}
        -DGFLAGS_INCLUDE_PATH=${GFLAGS_INCLUDE_DIR}
        -DBUILD_SHARED_LIBS=FALSE
        -DBUILD_BRPC_TOOLS=OFF
        -DGFLAGS_LIBRARY=${GFLAGS_LIBRARIES}

        -DLEVELDB_LIB=${LEVELDB_LIBRARIES}
        -DLEVELDB_INCLUDE_PATH=${LEVELDB_INCLUDE_DIR}

        -DPROTOC_LIB=${PROTOC_LIBRARY}
        -DPROTOBUF_LIBRARIES=${PROTOBUF_LIBRARY}
        -DProtobuf_LIBRARIES=${PROTOBUF_LIBRARY}
        -DPROTOBUF_INCLUDE_DIRS=${PROTOBUF_INCLUDE_DIR}
        -DProtobuf_INCLUDE_DIR=${PROTOBUF_INCLUDE_DIR}
        -DPROTOBUF_PROTOC_EXECUTABLE=${PROTOBUF_PROTOC}

        -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DPROJECT_BINARY_DIR=${LIB_INSTALL_PREFIX}
        -DWITH_GLOG=OFF
        -DDOWNLOAD_GTEST=OFF
        ${EXTERNAL_GENERATOR}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        BUILD_BYPRODUCTS ${BRPC_LIBRARIES}
)
