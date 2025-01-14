#!/bin/bash

BUILD_TYPE=release
CMAKE_FLAGS=""
MAKE_FLAGS=""
PREFIX="build"

source ../get_gitinfo_func.sh
GIT_INFO=$(get_gitinfo)
BUILD_TIME=$(echo $GIT_INFO | awk '{print $1}')
SHORT_COMMIT_ID=$(echo $GIT_INFO | awk '{print $2}')

echo "BUILD_TIME:" $BUILD_TIME
echo "COMMIT_ID:" $SHORT_COMMIT_ID

echo "BUILD_TYPE:" $BUILD_TYPE
echo "CMAKE_FLAGS:" $CMAKE_FLAGS
echo "MAKE_FLAGS:" $MAKE_FLAGS

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TIME=$BUILD_TIME -DGIT_COMMIT_ID=$SHORT_COMMIT_ID ${CMAKE_FLAGS} -S . -B ${PREFIX}