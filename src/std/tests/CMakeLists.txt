# Copyright (c) 2023-present, arana-db Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

CMAKE_MINIMUM_REQUIRED(VERSION 3.25)

INCLUDE(GoogleTest)
SET(CMAKE_CXX_STANDARD 20)

AUX_SOURCE_DIRECTORY(.. DIR_SRCS)

FILE(GLOB_RECURSE GTEST_TEST_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

FOREACH (std_test_source ${GTEST_TEST_SOURCE})
    GET_FILENAME_COMPONENT(std_test_filename ${std_test_source} NAME)
    STRING(REPLACE ".cc" "" std_test_name ${std_test_filename})

#    set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
    ADD_EXECUTABLE(${std_test_name} ${std_test_source})
    TARGET_INCLUDE_DIRECTORIES(${std_test_name}
            PRIVATE ${PROJECT_SOURCE_DIR}/src
            PRIVATE ${GTEST_INCLUDE_DIR}
            )

    ADD_DEPENDENCIES(${std_test_name} kstd gtest)
    TARGET_LINK_LIBRARIES(${std_test_name}
             PRIVATE kstd
             PRIVATE ${GTEST_LIBRARIES}
            )
    GTEST_DISCOVER_TESTS(${std_test_name})
ENDFOREACH ()
