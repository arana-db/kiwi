# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

cmake_minimum_required(VERSION 3.18)

include(GoogleTest)
set(CMAKE_CXX_STANDARD 20)

AUX_SOURCE_DIRECTORY(../ DIR_SRCS)

file(GLOB_RECURSE GTEST_TEST_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

foreach (gtest_test_source ${GTEST_TEST_SOURCE})
    get_filename_component(gtest_test_filename ${gtest_test_source} NAME)
    string(REPLACE ".cc" "" gtest_test_name ${gtest_test_filename})

    add_executable(${gtest_test_name} ${gtest_test_source} ${DIR_SRCS})
    target_include_directories(${gtest_test_name}
            PRIVATE ${GTEST_INCLUDE_DIR}
    )

    add_dependencies(${gtest_test_name} pstd gtest)
    target_link_libraries(${gtest_test_name}
            PUBLIC pstd
            PUBLIC gtest
    )
    gtest_discover_tests(${gtest_test_name})
endforeach ()
