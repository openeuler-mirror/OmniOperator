#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

targz_name=boostkit-omniop-operator-1.0.0-aarch64
zip_name=BoostKit-omniop_1.0.0

echo "Start build C++ modules"
cd core/build
echo "-- Enter" $(dirname $(readlink -f $0))
if [ "$1" = 'coverage' ]; then
    echo "-- Enable coverage"
    sh build.sh $*
    ./test/omtest --gtest_output=xml:test_detail.xml
    lcov --d ../ --c --output-file test.info --rc lcov_branch_coverage=1
    genhtml test.info -o test_coverage --branch-coverage --rc lcov_branch_coverage=1
else
    echo "-- Disable coverage"
    sh build.sh $*
    ./test/omtest --gtest_output=xml:test_detail.xml
fi

echo "Start build java modules"
cd ../../bindings/java
wget http://szxy1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public/com/huawei/devtest/devtestcov-maven-plugin/2.1.1/devtestcov-maven-plugin-2.1.1.jar --proxy=off
wget http://szxy1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/sz-maven-public/com/huawei/devtest/devtestcov-maven-plugin/2.1.1/devtestcov-maven-plugin-2.1.1.pom --proxy=off

mvn install:install-file -Dfile=devtestcov-maven-plugin-2.1.1.jar -DpomFile=devtestcov-maven-plugin-2.1.1.pom
rm -r devtestcov-maven-plugin-2.1.1.jar
rm -r devtestcov-maven-plugin-2.1.1.pom

if [ "$1" = 'coverage' ]; then
    echo "-- Enable coverage"
    mvn clean devtestcov:atest -Dactive.devtest=true -Dmaven.test.failure.ignore=true -Djacoco-agent.destfile=target/jacoco.exec
else
    echo "-- Disable coverage"
    mvn clean install
fi

if [ "$1" != 'coverage' ]; then
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
    tar -zcvf $targz_name.tar.gz $targz_name
    zip $zip_name.zip $targz_name.tar.gz
fi
