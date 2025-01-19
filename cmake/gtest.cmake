# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

SET(GTEST_SOURCES_DIR "${LIB_SOURCE_DIR}/gtest" CACHE PATH "Path to gtest sources")
SET(GTEST_INCLUDE_DIR "${LIB_INCLUDE_DIR}" CACHE PATH "gtest include directory." FORCE)
SET(GTEST_LIBRARIES "${LIB_INSTALL_DIR}/libgtest.a" CACHE FILEPATH "gtest lib directory." FORCE)
SET(GTEST_MAIN_LIBRARIES "${LIB_INSTALL_DIR}/libgtest_main.a" CACHE FILEPATH "gtest main include directory." FORCE)
SET(GMOCK_LIBRARIES "${LIB_INSTALL_DIR}/libgmock.a" CACHE FILEPATH "gmock directory." FORCE)

ExternalProject_Add(
        gtest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
        GIT_SHALLOW true
        SOURCE_DIR ${GTEST_SOURCES_DIR}
        CMAKE_ARGS
        ${EXTERNAL_GENERATOR}
        -DCMAKE_INSTALL_PREFIX=${LIB_INSTALL_PREFIX}
        -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        ${EXTERNAL_PROJECT_C}
        ${EXTERNAL_PROJECT_CXX}
        ${EXTERNAL_PROJECT_CXX_FLAGS}
        ${EXTERNAL_PROJECT_CXX_LINK_FLAGS}
        BUILD_COMMAND ${EXTERNAL_BUILD} -j${CPU_CORE}
        BUILD_BYPRODUCTS ${GTEST_LIBRARIES}
)
