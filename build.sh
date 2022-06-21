#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

targz_name=boostkit-omniop-operator-1.0.0-aarch64
zip_name=BoostKit-omniop_1.0.0

if [ "$1" = 'release' ]; then
  open_source_dir="open_source"
  mkdir -p ./${open_source_dir}
  cp -r ../huawei_secure_c ${open_source_dir}
  cp -r ../jemalloc ${open_source_dir}
  cp -r ../json ${open_source_dir}
  cp -r ../llvm-project ${open_source_dir}

  cd ${open_source_dir}

  echo "Start build huawei_secure_c"
  cd huawei_secure_c/src
  sudo make
  cd ../../
  sudo cp huawei_secure_c/lib/libsecurec.so $OMNI_HOME/lib
  sudo cp -r huawei_secure_c/include/ $OMNI_HOME/lib

  echo "Start jemalloc"
  cd jemalloc
  sudo ./autogen.sh --disable-initial-exec-tls
  sudo make -j16
  sudo make install

  echo "Start build json"
  cd ../json
  mkdir build
  cd build
  sudo cmake ../
  sudo make -j16
  sudo make install

  cd ../../../core/build
else
  cd core/build
fi

echo "Start build C++ modules using $1"
echo "-- Enter" $(dirname $(readlink -f $0))
if [ "$1" = 'coverage-c++' ]; then
    echo "-- Enable coverage for c++"
    sh build.sh coverage --disable-jit
    ./test/omtest --gtest_output=xml:test_detail.xml
    lcov --d ../ --c --output-file test.info --rc lcov_branch_coverage=1
    genhtml test.info -o test_coverage --branch-coverage --rc lcov_branch_coverage=1
else
    echo "-- Disable coverage for c++"
    sh build.sh release
    ./test/omtest --gtest_output=xml:test_detail.xml
fi

echo "Start build java modules using $1"
cd ../../bindings/java

if [ "$1" = 'coverage-java' ]; then
    echo "-- Enable coverage for java"
    mvn clean install devtestcov:atest -Dactive.devtest=true -Dmaven.test.failure.ignore=true -Djacoco-agent.destfile=target/jacoco.exec
elif [ "$1" = 'coverage-c++' ]; then
    echo "-- Disable coverage for java and skip tests"
    mvn clean install -DskipTests
else
    echo "-- Disable coverage for java"
    mvn clean install
fi

if [ "$1" = 'release' ]; then
    cd ../../
    # clean environment
    if [ -z "$OMNI_HOME" ]; then
      echo "OMNI_HOME is empty"
      package_files=/opt/lib
    else
      echo "OMNI_HOME = $OMNI_HOME"
      package_files=$OMNI_HOME/lib
    fi

    if [ -d "$targz_name" ]; then
      rm -rf $targz_name/
    fi

    echo mkdir -p $targz_name

    if [ -f "$targz_name.tar.gz" ]; then
      rm -rf $targz_name.tar.gz
    fi

    if [ -f "$zip_name.zip" ]; then
      rm -rf $zip_name.zip
    fi

    cp -r $package_files/ $targz_name
    cp bindings/java/target/*.jar $targz_name
    tar --owner root --group root -zcvf $targz_name.tar.gz $targz_name
    zip $zip_name.zip $targz_name.tar.gz
fi
