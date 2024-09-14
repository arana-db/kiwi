# Copyright (c) 2024-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

CMAKE_MINIMUM_REQUIRED(VERSION 3.25)

PROJECT(command)

message(STATUS "COMMAND_INCLUDE_DIR: ${COMMAND_INCLUDE_DIR}")
SET(CMAKE_CXX_STANDARD 20)

AUX_SOURCE_DIRECTORY(. COMMAND_SRC)
SET(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR})
ADD_LIBRARY(command ${COMMAND_SRC})

IF (CMAKE_BUILD_TYPE STREQUAL "Release")
    # get current date and time and git commit id
    EXECUTE_PROCESS(COMMAND date "+%Y-%m-%d_%H:%M:%S" OUTPUT_VARIABLE BUILD_TIMESTAMP OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(COMMAND git rev-parse HEAD OUTPUT_VARIABLE GIT_COMMIT_ID OUTPUT_STRIP_TRAILING_WHITESPACE)

    MESSAGE(STATUS "Build timestamp: ${BUILD_TIMESTAMP}")
    MESSAGE(STATUS "Git commit id: ${GIT_COMMIT_ID}")

    TARGET_COMPILE_DEFINITIONS(command PRIVATE Kiwi_BUILD_DATE="${BUILD_TIMESTAMP}" Kiwi_GIT_COMMIT_ID="${GIT_COMMIT_ID}")
ENDIF ()


TARGET_INCLUDE_DIRECTORIES(command
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/pstd
        PRIVATE ${PROJECT_SOURCE_DIR}/src/net
        PRIVATE ${PROJECT_SOURCE_DIR}/src/storage/include
        PRIVATE ${PROJECT_SOURCE_DIR}/src/command
        PRIVATE ${rocksdb_SOURCE_DIR}/
        PRIVATE ${rocksdb_SOURCE_DIR}/include
        PRIVATE ${BRAFT_INCLUDE_DIR}
        PRIVATE ${BRPC_INCLUDE_DIR}
        PRIVATE ${LIBEVENT_INCLUDE_DIRS}
)

TARGET_LINK_LIBRARIES(command
        net
        dl
        fmt
        rocksdb
        snappy
        lz4
        zstd
        storage
        gflags
        spdlog
        pstd
        braft
        brpc
        ssl
        crypto
        zlib
        protobuf
        leveldb
        z
        praft
        praft_pb
        binlog_pb
        "${LIB}"
)