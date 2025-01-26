# Copyright (c) 2023-present, arana-db Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

FILE(GLOB STORAGE_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/include/storage/*.h"
)
SET(LIBRARY_OUTPUT_PATH ${PLIB_INSTALL_DIR})
ADD_LIBRARY(storage ${STORAGE_SRC})

TARGET_INCLUDE_DIRECTORIES(storage
        PUBLIC ${CMAKE_SOURCE_DIR}/src
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${ROCKSDB_SOURCES_DIR}
        PRIVATE ${ROCKSDB_SOURCES_DIR}/include
        PRIVATE ${PROTO_OUTPUT_DIR}
        PRIVATE ${PROTOBUF_INCLUDE_DIR}
        PRIVATE ${SPDLOG_INCLUDE_DIR}
        PRIVATE ${PSTD_INCLUDE_DIR}
)

TARGET_LINK_LIBRARIES(storage
       PRIVATE kstd
       PRIVATE ${FMT_LIBRARIES}
       PRIVATE ${ZLIB_LIBRARIES}
       PRIVATE ${LEVELDB_LIBRARIES}
       PRIVATE ${GFLAGS_LIBRARIES}
       PRIVATE ${ROCKSDB_LIBRARIES}
       PRIVATE ${PROTOBUF_LIBRARY}
       PRIVATE ${SPDLOG_LIBRARIES}
)

SET_TARGET_PROPERTIES(storage PROPERTIES LINKER_LANGUAGE CXX)

ADD_SUBDIRECTORY(tests)

ADD_DEPENDENCIES(storage
        kstd
        zlib
        leveldb
        gflags
        rocksdb
        protobuf
        spdlog
)
