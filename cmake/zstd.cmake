# Copyright (c) 2023-present, OpenAtom Foundation, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(zstd_INCLUDE_DIRS "${LIB_INCLUDE_DIR}" CACHE PATH "zstd include directory." FORCE)
SET(zstd_LIBRARIES "${LIB_INSTALL_DIR}/libzstd.a" CACHE FILEPATH "zstd include directory." FORCE)

ExternalProject_Add(
        extern_zstd
        ${EXTERNAL_PROJECT_LOG_ARGS}
        URL https://github.com/facebook/zstd/releases/download/v1.5.4/zstd-1.5.4.tar.gz
        URL_HASH SHA256=0f470992aedad543126d06efab344dc5f3e171893810455787d38347343a4424
        SOURCE_SUBDIR build/cmake
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
        -DBUILD_TESTING=OFF
        -DZSTD_BUILD_STATIC=ON
        -DZSTD_BUILD_SHARED=OFF
        BUILD_COMMAND make -j${CPU_CORE}
)

ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET zstd PROPERTY IMPORTED_LOCATION ${zstd_LIBRARIES})
ADD_DEPENDENCIES(zstd extern_zstd)
