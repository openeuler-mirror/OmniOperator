#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

# prepare java file
wget http://szxy1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public/com/huawei/devtest/devtestcov-maven-plugin/2.1.1/devtestcov-maven-plugin-2.1.1.jar --proxy=off
wget http://szxy1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public/com/huawei/devtest/devtestcov-maven-plugin/2.1.1/devtestcov-maven-plugin-2.1.1.pom --proxy=off

mvn install:install-file -Dfile=devtestcov-maven-plugin-2.1.1.jar -DpomFile=devtestcov-maven-plugin-2.1.1.pom
rm -r devtestcov-maven-plugin-2.1.1.jar
rm -r devtestcov-maven-plugin-2.1.1.pom

# clean environment
if [ -z "$OMNI_HOME" ]; then
  echo "OMNI_HOME is empty"
  OMNI_HOME=/opt
fi

echo "OMNI_HOME = $OMNI_HOME"
lib_home=$OMNI_HOME/lib
[ ! -d "lib_home" ] && echo mkdir -p $lib_home

echo "lib_home = $lib_home, LD_LIBRARY_PATH = $LD_LIBRARY_PATH"

rm -rf $lib_home/*.so $lib_home/ir $lib_home/jit_libs
echo "-- Enter" $(dirname $(readlink -f $0))
cd $(dirname $(readlink -f $0))
rm -rf $(ls | grep -v "build.sh")

print_gcc_lib() {
  gcc -print-search-dirs | sed '/^lib/b 1;d;:1;s,/[^/.][^/]*/\.\./,/,;t 1;s,:[^=]*=,:;,;s,;,;  ,g' | tr \; \\012
}
#append_options
append_options()
{
  if [ $# = 1 ]; then
    echo "-- Enable Debug"
  elif [ "$2" = 'all' ]; then
    echo "-- Enable All Module Debug, Include: OPERATOR,VECTOR,LLVM"
    options="$options -DDEBUG_OPERATOR=ON -DDEBUG_VECTOR=ON -DDEBUG_LLVM=ON"
  else
    for i in $* ; do
        if [ "$i" != "$1" ]; then
            if [ "$i" = 'op' ]; then
              echo "-- Enable Operator Debug"
              options="$options -DDEBUG_OPERATOR=ON"
            elif [ "$i" = 'vec' ]; then
              echo "-- Enable Vector Debug"
              options="$options -DDEBUG_VECTOR=ON"
            elif [ "$i" = 'llvm' ]; then
              echo "-- Enable LLVM Debug"
              options="$options -DDEBUG_LLVM=ON"
            elif [ "$i" = '--disable-jit' ]; then
              echo "-- Disable JIT"
              options="$options -DDISABLE_JIT=ON"
            fi
        fi
    done
  fi
}

# options
if [ $# != 0 ] ; then
  options=""
  if [ "$1" = 'debug' ]; then
    echo "-- Enable Debug"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG=ON"
    append_options $*
  elif [ "$1" = 'trace' ]; then
    echo "-- Enable Trace"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DTRACE=ON"
    append_options $*
  elif [ "$1" = 'release' ];then
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
    if [ "$2" = '--disable-jit' ]; then
      echo "-- Disable JIT"
      options="$options -DDISABLE_JIT=ON"
    fi
  elif [ "$1" = 'coverage' ]; then
      echo "-- Enable Coverage"
      options="$options -DCMAKE_BUILD_TYPE=Debug -DCOVERAGE=ON"
      append_options $*
  fi
  print_gcc_lib
  cmake ../ $options
else
  echo "-- Enable Release"
  print_gcc_lib
  cmake ../ -DCMAKE_BUILD_TYPE=Release
fi
make clean
make -j16
make install
