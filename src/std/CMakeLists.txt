# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

AUX_SOURCE_DIRECTORY(. STD_SRC)
SET(LIBRARY_OUTPUT_PATH ${PLIB_INSTALL_DIR})
SET(PSTD_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE PATH "kstd include directory." FORCE)
ADD_LIBRARY(kstd ${STD_SRC})

ADD_SUBDIRECTORY(tests)

TARGET_INCLUDE_DIRECTORIES(kstd
        PRIVATE ${ROCKSDB_SOURCES_DIR}/include
        PRIVATE ${LIB_INCLUDE_DIR}
        PRIVATE ${PSTD_INCLUDE_DIR}
)

TARGET_LINK_LIBRARIES(kstd;
        PRIVATE ${SPDLOG_LIBRARIES}
        PRIVATE pthread
        PRIVATE ${FMT_LIBRARIES}
)

SET_TARGET_PROPERTIES(kstd PROPERTIES LINKER_LANGUAGE CXX)
ADD_DEPENDENCIES(kstd rocksdb spdlog)
