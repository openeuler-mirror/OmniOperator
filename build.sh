#!/bin/bash
# build file for OmniOperatorJit
# Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.

set -e

source $(cd $(dirname ${BASH_SOURCE[0]}) && pwd)/env_check.sh

TARGZ_NAME=boostkit-omniop-operator-1.2.0-aarch64
ZIP_NAME=BoostKit-omniop_1.2.0

# if either help or --help is provided, the usage should be printed prior to exit
if [ "$1" = 'help' ] || [ "$1" = '--help' ]; then
  print_usage
  exit 0
fi

# check if required env vars are set
check_java_home
check_omni_home

### main build begins here ###
echo "Start building modules using $1"
echo "-- Enter" $(dirname $(readlink -f $0))

# save working directory
CWD=$(pwd)

# check for $1 param
case "$1" in
  release)
    setup_dependencies

    echo "-- Only build"
    cd ${CWD} && build release:java --enable-hmpp

    cd $CWD/bindings/java && mvn clean install -Domni.home=$OMNI_HOME
    cd $CWD/core/src/udf/java && mvn clean install

    cd $CWD
    # clean environment
    [ -d "$TARGZ_NAME" ] && rm -rf $TARGZ_NAME
    [ -f "$TARGZ_NAME.tar.gz" ] && rm -rf $TARGZ_NAME.tar.gz
    [ -f "$ZIP_NAME.zip" ] && rm -rf $ZIP_NAME.zip

    cp -r $OMNI_HOME/lib $TARGZ_NAME
    cp $CWD/bindings/java/target/*.jar $TARGZ_NAME
    cp $CWD/core/src/udf/java/target/*.jar $TARGZ_NAME
    tar --owner root --group root -zcvf $TARGZ_NAME.tar.gz $TARGZ_NAME
    zip $ZIP_NAME.zip $TARGZ_NAME.tar.gz
    ;;
  test)
    setup_dependencies

    echo "-- Enable build and test"
    cd ${CWD} && build release:java --enable-hmpp
    $CWD/build/core/test/omtest --gtest_output=xml:test_detail.xml

    cd $CWD/bindings/java && mvn clean install -Domni.home=$OMNI_HOME -DskipTests
    cd $CWD/core/src/udf/java && mvn clean install
    ;;
  coverage-java)
    echo "-- Enable coverage for java"
    cd ${CWD} && build release:java --enable-hmpp

    $CWD/build/core/test/omtest --gtest_output=xml:${CWD}/core/build/test_detail.xml

    cd $CWD/bindings/java && mvn clean install devtestcov:atest -Domni.home=$OMNI_HOME -Dactive.devtest=true -Dmaven.test.failure.ignore=true -Djacoco-agent.destfile=target/jacoco.exec
    cd $CWD/core/src/udf/java && mvn clean install
    ;;
  coverage-c++)
    echo "-- Enable coverage for c++"
    cd ${CWD} && build coverage:java --enable-hmpp

    $CWD/build/core/test/omtest --gtest_output=xml:${CWD}/core/build/test_detail.xml

    lcov --d $CWD/build --c --output-file test.info --rc lcov_branch_coverage=1
    lcov --remove test.info '*/opt/buildtools/include/*' '*/usr/include/*' '*/usr/lib/*' '*/usr/lib64/*' '*/usr/local/include/*' '*/usr/local/lib/*' '*/usr/local/lib64/*' '*/test/*' -o final.info --rc lcov_branch_coverage=1
    genhtml final.info -o ${CWD}/core/build/test_coverage --branch-coverage --rc lcov_branch_coverage=1

    cd $CWD/bindings/java && mvn clean install -Domni.home=$OMNI_HOME -DskipTests
    cd $CWD/core/src/udf/java && mvn clean install
    ;;
  *)
    echo "-- Enable default options"
    cd ${CWD} && build release:java --enable-hmpp
    $CWD/build/core/test/omtest --gtest_output=xml:${CWD}/core/build/test_detail.xml

    cd $CWD/bindings/java && mvn clean -Domni.home=$OMNI_HOME install
    cd $CWD/core/src/udf/java && mvn clean install
    ;;
esac
