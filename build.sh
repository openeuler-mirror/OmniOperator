#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

targz_name=boostkit-omniop-operator-1.2.0-aarch64
zip_name=BoostKit-omniop_1.2.0

if [ "$1" = 'release' ] || [ "$1" = 'test' ]; then
  open_source_dir="open_source"
  mkdir -p ./${open_source_dir}
  cp -r ../huawei_secure_c ${open_source_dir}
  cp -r ../jemalloc ${open_source_dir}
  cp -r ../json ${open_source_dir}
  cp -r ../llvm-project ${open_source_dir}
  cp -r ../googletest ${open_source_dir}/benchmark
  cp -r ../boost ${open_source_dir}

  echo "Start build open source code for huawei_secure_c, jemalloc and json"
  cd ${open_source_dir}/huawei_secure_c/src
  sudo make
  cd ../../
  sudo cp huawei_secure_c/lib/libsecurec.so $OMNI_HOME/lib
  sudo cp -r huawei_secure_c/include/ $OMNI_HOME/lib

  cd jemalloc
  sudo ./autogen.sh --disable-initial-exec-tls
  sudo make -j16 && sudo make install

  mkdir ../json/build
  cd ../json/build
  sudo cmake ../
  sudo make -j16 && sudo make install

  cd ../../../../boost
  sudo chmod -R 755 ./tools
  dos2unix ./bootstrap.sh
  dos2unix ./tools/build/src/engine/build.sh
  sudo /bin/bash ./bootstrap.sh
  sudo ./b2 headers install

  cd ../OmniOperatorJIT/core/build
else
  cd core/build
fi

echo "Start building modules using $1"
echo "-- Enter" $(dirname $(readlink -f $0))

if [ "$1" = 'coverage-java' ]; then
    echo "-- Enable coverage for java"
    sh build.sh release --enable-hmpp
    ./test/omtest --gtest_output=xml:test_detail.xml

    cd ../../bindings/java
    mvn clean install devtestcov:atest -Dactive.devtest=true -Dmaven.test.failure.ignore=true -Djacoco-agent.destfile=target/jacoco.exec
    cd ../../core/src/udf/java
    mvn clean install
elif [ "$1" = 'coverage-c++' ]; then
    echo "-- Enable coverage for c++"
    sh build.sh coverage --enable-hmpp
    ./test/omtest --gtest_output=xml:test_detail.xml
    lcov --d ../ --c --output-file test.info --rc lcov_branch_coverage=1
    lcov --remove test.info '*/opt/buildtools/include/*' '*/usr/include/*' '*/usr/lib/*' '*/usr/lib64/*' '*/usr/local/include/*' '*/usr/local/lib/*' '*/usr/local/lib64/*' '*/test/*' -o final.info --rc lcov_branch_coverage=1
    genhtml final.info -o test_coverage --branch-coverage --rc lcov_branch_coverage=1

    cd ../../bindings/java
    mvn clean install -DskipTests
    cd ../../core/src/udf/java
    mvn clean install
elif [ "$1" = 'release' ]; then
    echo "-- Only build"
    sh build.sh release --enable-hmpp

    cd ../../bindings/java
    mvn clean install
    cd ../../core/src/udf/java
    mvn clean install
elif [ "$1" = 'test' ]; then
    echo "-- Enable build and test"
    sh build.sh release --enable-hmpp
    ./test/omtest --gtest_output=xml:test_detail.xml

    cd ../../bindings/java
    mvn clean install -DskipTests
    cd ../../core/src/udf/java
    mvn clean install
else
    echo "-- Enable default options"
    sh build.sh release --enable-hmpp
    ./test/omtest --gtest_output=xml:test_detail.xml

    cd ../../bindings/java
    mvn clean install
    cd ../../core/src/udf/java
    mvn clean install
fi

if [ "$1" = 'release' ]; then
    cd ../../../../
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
    cp core/src/udf/java/target/*.jar $targz_name
    tar --owner root --group root -zcvf $targz_name.tar.gz $targz_name
    zip $zip_name.zip $targz_name.tar.gz
fi
