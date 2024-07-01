# Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

FILE(GLOB CACHE_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
ADD_LIBRARY(cache ${CACHE_SRC})

add_dependencies(cache net protobuf gflags rediscache)

TARGET_INCLUDE_DIRECTORIES(cache
        PRIVATE ${PROJECT_SOURCE_DIR}/src
        PRIVATE ${rocksdb_SOURCE_DIR}/include
        PRIVATE ${REDISCACHE_INCLUDE_DIR}
)

target_link_libraries(cache
        PUBLIC ${GTEST_LIBRARY}
        PUBLIC pstd
        PUBLIC ${ROCKSDB_LIBRARY}
        PUBLIC storage
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${REDISCACHE_LIBRARY}
        )
             
TARGET_LINK_LIBRARIES(praft;storage; pstd rediscache gflags rocksdb)
        
SET_TARGET_PROPERTIES(cache PROPERTIES LINKER_LANGUAGE CXX)