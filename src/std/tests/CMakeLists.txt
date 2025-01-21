# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

cmake_minimum_required(VERSION 3.18)

include(GoogleTest)
set(CMAKE_CXX_STANDARD 20)

aux_source_directory(.. DIR_SRCS)

file(GLOB_RECURSE PSTD_TEST_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

foreach (std_test_source ${PSTD_TEST_SOURCE})
    get_filename_component(std_test_filename ${std_test_source} NAME)
    string(REPLACE ".cc" "" std_test_name ${std_test_filename})

#    set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
    add_executable(${std_test_name} ${std_test_source})
    target_include_directories(${std_test_name}
            PRIVATE ${PROJECT_SOURCE_DIR}/src
            PRIVATE ${GTEST_INCLUDE_DIR}
            )

    add_dependencies(${std_test_name} kstd gtest)
    target_link_libraries(${std_test_name}
             PRIVATE kstd
             PRIVATE ${GTEST_LIBRARIES}
            )
    gtest_discover_tests(${std_test_name})
endforeach ()
