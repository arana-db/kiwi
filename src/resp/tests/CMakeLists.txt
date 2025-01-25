# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

CMAKE_MINIMUM_REQUIRED(VERSION 3.25)

INCLUDE(GoogleTest)
SET(CMAKE_CXX_STANDARD 20)

AUX_SOURCE_DIRECTORY(../ DIR_SRCS)

FILE(GLOB_RECURSE GTEST_TEST_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

FOREACH (gtest_test_source ${GTEST_TEST_SOURCE})
    GET_FILENAME_COMPONENT(gtest_test_filename ${gtest_test_source} NAME)
    STRING(REPLACE ".cc" "" gtest_test_name ${gtest_test_filename})

    ADD_EXECUTABLE(${gtest_test_name} ${gtest_test_source} ${DIR_SRCS})
    TARGET_INCLUDE_DIRECTORIES(${gtest_test_name}
            PRIVATE ${GTEST_INCLUDE_DIR}
    )

    ADD_DEPENDENCIES(${gtest_test_name} kstd gtest)
    TARGET_LINK_LIBRARIES(${gtest_test_name}
            PUBLIC kstd
            PUBLIC gtest
    )
    GTEST_DISCOVER_TESTS(${gtest_test_name})
ENDFOREACH ()
