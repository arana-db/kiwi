# Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  ExternalProject_Add(jemalloc
    DEPENDS
    URL
    https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz
    URL_HASH
    MD5=594dd8e0a1e8c1ef8a1b210a1a5aff5b
    DOWNLOAD_NO_PROGRESS
    1
    UPDATE_COMMAND
    ""
    LOG_CONFIGURE
    1
    LOG_BUILD
    1
    LOG_INSTALL
    1
    CONFIGURE_COMMAND
    <SOURCE_DIR>/autogen.sh --prefix=${LIB_INSTALL_PREFIX}
    BUILD_IN_SOURCE
    1
    BUILD_COMMAND
    make -j${CPU_CORE}
    BUILD_ALWAYS
    1
    INSTALL_COMMAND
    make install
  )

  set(JEMALLOC_LIBRARY ${LIB_INSTALL_DIR}/libjemalloc.a)
  set(JEMALLOC_INCLUDE_DIR ${LIB_INCLUDE_DIR})
  set(LIBJEMALLOC_NAME jemalloc)
  set(JEMALLOC_ON ON)
else()
  set(JEMALLOC_ON OFF)
endif()