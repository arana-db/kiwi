# Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#####################################################################################
# This file is used to generate the list of source files for the gtest target.
# It is included in the gtest/CMakeLists.txt file.
# Add the source files in src to DIR_SRCS that will be used in any tests
#####################################################################################

list(APPEND DIR_SRCS ${PROJECT_SOURCE_DIR}/src/resp2_parse.cc)
