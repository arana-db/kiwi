#!/bin/bash

# Color codes
C_RED="\033[31m"
C_GREEN="\033[32m"
C_END="\033[0m"

# Build configuration
K_VERSION="1.0.0"
BUILD_TYPE="Release"
VERBOSE=0
CMAKE_FLAGS=""
MAKE_FLAGS=""
PREFIX="cmake-build"

PWD=$(pwd)
PROJECT_HOME="${PWD}/"
CONF="${PROJECT_HOME}/etc/conf/kiwi.conf"

# Get build time and commit ID
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BUILD_TIME="unknown"
SHORT_COMMIT_ID="unknown"

function get_git_info() {
    time=$(git log -1 --format=%ai 2>/dev/null || echo "unknown")
    if [ "$time" != "unknown" ]; then
        time=${time:0:10}
    fi
    export BUILD_TIME=$time

    id=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    id=$([ "$id" != "unknown" ] && echo ${id:0:8} || echo "unknown")
    if [ "$id" = "unknown" ]; then
        echo "no git commit id"
        id="kiwi"
    fi
    export SHORT_COMMIT_ID=$id
    # echo "$BUILD_TIME $SHORT_COMMIT_ID"
}

function build() {
    if [ ! -f "/proc/cpuinfo" ]; then
        CPU_CORE=$(sysctl -n hw.ncpu)
    else
        CPU_CORE=$(cat /proc/cpuinfo | grep "processor" | wc -l)
    fi
    if [ ${CPU_CORE} -eq 0 ]; then
        CPU_CORE=1
    fi

    echo "CPU cores: ${CPU_CORE}"
    echo "BUILD_TIME: $BUILD_TIME"
    echo "COMMIT_ID: $SHORT_COMMIT_ID"
    echo "BUILD_TYPE: $BUILD_TYPE"
    echo "CMAKE_FLAGS: $CMAKE_FLAGS"
    echo "MAKE_FLAGS: $MAKE_FLAGS"

    if [ "${BUILD_TYPE}" == "Release" ]; then
        PREFIX="${PREFIX}-release"
    else
        PREFIX="${PREFIX}-debug"
    fi

    mkdir -p download
    mkdir -p ${PREFIX}
    cp CMakeLists.txt ${PREFIX}/

    cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
          -DK_VERSION=$K_VERSION \
          -DBUILD_TIME=$BUILD_TIME \
          -DGIT_COMMIT_ID=$SHORT_COMMIT_ID \
          -DKIWI_BUILD_DATE="$BUILD_TIME" \
          -DKIWI_GIT_COMMIT_ID="$SHORT_COMMIT_ID" \
          ${CMAKE_FLAGS} -S . -B ${PREFIX}
    cmake --build ${PREFIX} -- ${MAKE_FLAGS} -j ${CPU_CORE}

    if [ $? -eq 0 ]; then
        echo -e "kiwi compile complete, output file ${C_GREEN} ./bin/kiwi ${C_END}"
    else
        echo -e "${C_RED} kiwi compile fail ${C_END}"
        exit 1
    fi
}

# ":?" makes sure the bash var is not empty.
function clear() {
  rm -rf ${PROJECT_HOME:?}/deps-debug
  rm -rf ${PROJECT_HOME:?}/deps-release
  rm -rf ${PROJECT_HOME:?}/cmake-build-debug
  rm -rf ${PROJECT_HOME:?}/cmake-build-release
  rm -rf ${PROJECT_HOME:?}/cmake-build-release-release
  rm -rf ${PROJECT_HOME:?}/build
  rm -rf ${PROJECT_HOME:?}/download
  rm -rf ${PROJECT_HOME:?}/build-release
  rm -rf ${PROJECT_HOME:?}/build-debug
  rm -rf ${PROJECT_HOME:?}/bin
  exit 0
}

function show_help() {
  echo "
  sh $0 --gitinfo   get git info
  sh $0 --debug     compile with debug
  sh $0 --clang     use clang compiler
  sh $0 --kiwi      only compile kiwi
  sh $0 --clear     clear compilation directory
  sh $0 -h|--help   show help
  sh $0 --prefix    compile output path
  sh $0 --verbose   compile with verbose
  "
  exit 0
}

# ARGS=`getopt -a -o h -l help,debug,verbose,prefix: -- "$@"`
# Convert the parsed arguments into an array
# eval set -- "$ARGS"

while true; do
  # echo "hello first arg $1"
  case "$1" in
  -c|--clear)
    clear
    shift ;;

  --gitinfo)
    get_git_info
    shift ;;

  --debug)
    BUILD_TYPE=debug
    ;;

  --clang)
    CMAKE_FLAGS="${CMAKE_FLAGS} -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++"
    ;;

  --kiwi)
    MAKE_FLAGS="${MAKE_FLAGS} kiwi"
    ;;

  -h|--help)
    show_help
    ;;

  --prefix)
    if [[ -n $2 ]]; then
      PREFIX=$2
    else
      echo "you should provide a value for --prefix as output path"
      exit 0
    fi
    shift;;

  --verbose)
    CMAKE_FLAGS="${CMAKE_FLAGS} -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON"
    MAKE_FLAGS="${MAKE_FLAGS} VERBOSE=1"
    ;;

  --)
    shift
    break;;

  *)
    break;;
  esac
  shift
done

get_git_info
build

