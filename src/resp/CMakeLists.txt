# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

CMAKE_MINIMUM_REQUIRED(VERSION 3.25)

PROJECT(resp)

message(STATUS "PSTD_INCLUDE_DIR1111: ${PSTD_INCLUDE_DIR}")

AUX_SOURCE_DIRECTORY(. RESP_SRC)

SET(LIBRARY_OUTPUT_PATH ${PLIB_INSTALL_DIR})

SET(RESP_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE PATH "resp include directory." FORCE)

ADD_LIBRARY(resp ${RESP_SRC})

TARGET_LINK_LIBRARIES(resp kstd fmt)

ADD_SUBDIRECTORY(tests)

TARGET_INCLUDE_DIRECTORIES(resp
        PRIVATE ${PSTD_INCLUDE_DIR}
        PRIVATE ${FMT_INCLUDE_DIR}
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

ADD_DEPENDENCIES(resp kstd fmt)
