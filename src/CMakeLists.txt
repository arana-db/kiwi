# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

AUX_SOURCE_DIRECTORY(. KIWI_SRC)

ADD_EXECUTABLE(kiwi ${KIWI_SRC})

IF (CMAKE_BUILD_TYPE STREQUAL "Release")
    # get current date and time and git commit id
    EXECUTE_PROCESS(COMMAND date "+%Y-%m-%d_%H:%M:%S" OUTPUT_VARIABLE BUILD_TIMESTAMP OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(COMMAND git rev-parse HEAD OUTPUT_VARIABLE GIT_COMMIT_ID OUTPUT_STRIP_TRAILING_WHITESPACE)

    MESSAGE(STATUS "Build timestamp: ${BUILD_TIMESTAMP}")
    MESSAGE(STATUS "Git commit id: ${GIT_COMMIT_ID}")

    TARGET_COMPILE_DEFINITIONS(kiwi
            PRIVATE Kkiwi_GIT_COMMIT_ID="${GIT_COMMIT_ID}"
            PRIVATE Kkiwi_BUILD_DATE="${BUILD_TIMESTAMP}"
    )
ENDIF ()

# SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin/)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin/)

TARGET_INCLUDE_DIRECTORIES(kiwi
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${PROJECT_SOURCE_DIR}/src/std
        PRIVATE ${PROJECT_SOURCE_DIR}/src/net
        PRIVATE ${PROJECT_SOURCE_DIR}/src/storage/include
        PRIVATE ${rocksdb_SOURCE_DIR}/
        PRIVATE ${rocksdb_SOURCE_DIR}/include
        PRIVATE ${BRAFT_INCLUDE_DIR}
        PRIVATE ${BRPC_INCLUDE_DIR}
        PRIVATE ${RESP_INCLUDE_DIR}
        PRIVATE ${LIBEVENT_INCLUDE_DIRS}
        PRIVATE ${OPENSSL_INCLUDE_DIR}
)

ADD_DEPENDENCIES(kiwi
        gflags
        gtest
        fmt
        snappy
        zlib
        rocksdb
        protobuf
        kstd
        braft
        brpc
        storage
        resp
        leveldb
        raft
        raft_pb
        binlog_pb
        OpenSSL
)

IF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    SET(EXTRA_LINK_LIB 
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop"
    )
ENDIF()

TARGET_LINK_LIBRARIES(kiwi
        net
        dl
        storage
        kstd
        raft
        raft_pb
        binlog_pb
        resp
        ${FMT_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        ${SPDLOG_LIBRARIES}
        ${BRAFT_LIBRARIES}
        ${PROTOBUF_LIBRARY}
        ${LZ4_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${OPENSSL_SSL_LIBRARY}
        ${LEVELDB_LIBRARIES}
        ${Snappy_LIBRARIES}
        ${zstd_LIBRARIES}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${EXTRA_LINK_LIB}
)

SET_TARGET_PROPERTIES(kiwi PROPERTIES LINKER_LANGUAGE CXX)
