# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

INCLUDE(GoogleTest)

FILE(GLOB_RECURSE TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*test.cc")

FOREACH (TEST_SOURCE ${TEST_SOURCES})
    GET_FILENAME_COMPONENT(TEST_FILENAME ${TEST_SOURCE} NAME)
    STRING(REPLACE ".cc" "" TEST_NAME ${TEST_FILENAME})

    ADD_EXECUTABLE(${TEST_NAME} ${TEST_SOURCE})

    TARGET_INCLUDE_DIRECTORIES(${TEST_NAME}
            PRIVATE storage
            PRIVATE ${ROCKSDB_SOURCES_DIR}
            PRIVATE ${ROCKSDB_SOURCES_DIR}/include
            PRIVATE ${BRAFT_INCLUDE_DIR}
            PRIVATE ${PROTOBUF_INCLUDE_DIR}
            PRIVATE ${PROTO_OUTPUT_DIR}
            PRIVATE ${GTEST_INCLUDE_DIR}
    )

    TARGET_LINK_LIBRARIES(${TEST_NAME}
            PRIVATE storage
            PRIVATE ${GTEST_LIBRARIES}
            PRIVATE ${GTEST_MAIN_LIBRARIES}
            PRIVATE ${FMT_LIBRARIES}
            PRIVATE ${SPDLOG_LIBRARIES}
            PRIVATE kstd
            PRIVATE ${ROCKSDB_LIBRARIES}
            PRIVATE ${Snappy_LIBRARIES}
            PRIVATE ${LZ4_LIBRARIES}
            PRIVATE ${zstd_LIBRARIES}
            PRIVATE binlog_pb
    )
    GTEST_DISCOVER_TESTS(${TEST_NAME})
ENDFOREACH ()
