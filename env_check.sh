#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

# print_usage
print_usage() {
  echo "
  Usage:
    build.sh [type]

  Types:
    [default]                          = Enable Default Options (equivalent to Release)
    package                            = Enable Package
    release                            = Enable Release
    test                               = Enable Build and Test
    coverage-java                      = Enable Enable coverage for Java
    coverage-c++                       = Enable Enable coverage for C++
  "
}

# if JAVA_HOME is not set,
# prompt to set to detected location before exiting
check_java_home() {
  if [ -z "$JAVA_HOME" ]; then
    local java=$(which java|xargs readlink -f)

    echo "ERROR: JAVA_HOME is not set!"
    echo "If it's ${java%/bin/java},"
    echo "you can set it as follows:"
    echo "export JAVA_HOME=${java%/bin/java}"
    echo ""
    echo "Please set JAVA_HOME and try again"
    exit 1
  fi
}

# if OMNI_HOME is not set,
# prompt to set to suggested location before exiting
check_omni_home() {
  if [ -z "$OMNI_HOME" ]; then
    echo "ERROR: OMNI_HOME is not set!"
    echo "You can set it as follows:"
    echo "for system level configuration (requiring root privilege),"
    echo "export OMNI_HOME=/opt"
    echo "or for user level configuration,"
    echo "export OMNI_HOME=$HOME/opt"
    echo ""
    echo "Please set OMNI_HOME and try again"
    exit 1
  fi
}

setup_build() {
  echo "OMNI_HOME = $OMNI_HOME"
  [ ! -d "$OMNI_HOME/lib" ] && mkdir -p $OMNI_HOME/lib

  echo "LIB_HOME = $OMNI_HOME/lib, LD_LIBRARY_PATH = $LD_LIBRARY_PATH"
  rm -rf  $OMNI_HOME/lib/libboostkit*.so $OMNI_HOME/*-binding
}

# the current solution is to build from source manually
# the future plan is to use conan to manage all the dependencies
setup_dependencies() {
  local open_source_dir="open_source"
  local workspace=$(pwd)
  mkdir -p ${workspace}/${open_source_dir}

  cp -r ${workspace}/../libboundscheck ${open_source_dir}
  cp -r ${workspace}/../json ${open_source_dir}
  if [ "$1" != "package" ] && [ "$1" != "release" ]; then
    cp -r ${workspace}/../benchmark ${open_source_dir}
    cp -r ${workspace}/../googletest ${open_source_dir}/benchmark
  fi

  echo "Start build fmt.so"
  local fmt_so_core="${OMNI_HOME}/lib/libfmt.so.10"
  local fmt_tag="10.1.1"
  local fmt_repo="https://gitee.com/mirrors/fmt.git"
  local fmt_source_dir="${workspace}/${open_source_dir}/fmt"
  local fmt_build_dir="${fmt_source_dir}/build"

  # Check if the core fmt so file exists
  if [ ! -f "${fmt_so_core}" ]; then
    echo ">>>>> libfmt.so.10 not found in ${OMNI_HOME}/lib, start to clone fmt-${fmt_tag} source code and build..."
    rm -rf ${fmt_source_dir} && mkdir -p ${fmt_source_dir}
    git clone --branch ${fmt_tag} --depth=1 ${fmt_repo} ${fmt_source_dir}
    cd ${fmt_source_dir}
    mkdir -p build && cd build
    # Cmake build with your specified params
    cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DFMT_TEST=OFF \
    -DFMT_DOC=OFF \
    -DFMT_INSTALL=ON \
    -DBUILD_SHARED_LIBS=ON
    make -j$(nproc)
    sudo make install
    # Copy all generated fmt so files to OMNI_HOME/lib directly
    sudo cp -f libfmt.so* ${OMNI_HOME}/lib/
    echo ">>>>> All fmt shared libraries copied to ${OMNI_HOME}/lib successfully."
    # Back to workspace
    cd ${workspace}
  else
    echo ">>>>> libfmt.so.10 already exists in ${OMNI_HOME}/lib, skip fmt build process."
  fi

  echo "Start build folly"
  local folly_tag="v2024.07.01.00"
  local folly_repo="https://gitee.com/mirrors/folly.git"
  local folly_source_dir="${workspace}/${open_source_dir}/folly"
  echo ">>>>> Start to clone folly-${folly_tag} source code and build..."
  rm -rf ${folly_source_dir} && mkdir -p ${folly_source_dir}
  git clone --branch ${folly_tag} --depth=1 ${folly_repo} ${folly_source_dir}
  cd ${folly_source_dir}
  mkdir -p build && cd build
  cmake .. -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
  make -j$(nproc)
  sudo make install
  echo ">>>>> folly-${folly_tag} build and install completed successfully."
  cd ${workspace}

  echo "Start build open source code for libboundscheck, json and gtest"
  cd ${workspace}/${open_source_dir}/libboundscheck
  sudo make CC=gcc
  cd ${workspace}/${open_source_dir}
  sudo cp libboundscheck/lib/libboundscheck.so $OMNI_HOME/lib
  sudo cp -r libboundscheck/include/ $OMNI_HOME/lib

  mkdir ${workspace}/${open_source_dir}/json/build
  cd ${workspace}/${open_source_dir}/json/build && sudo cmake ../ && sudo make -j16 && sudo make install

  if [ "$1" != "package" ] && [ "$1" != "release" ]; then
    cd ${workspace}/${open_source_dir}/benchmark
    cmake -E make_directory "build"
    cmake -E chdir "build" cmake -DCMAKE_BUILD_TYPE=Release ../
    sudo cmake --build "build" --config Release --target install
  fi
}

# package build_script/build.sh functionality here
build() {
  local workspace=$(pwd)
  export OMNI_COMPILER_THREAD_COUNT=8
  sh $workspace/build_scripts/build.sh "$@"
  cd $workspace
}