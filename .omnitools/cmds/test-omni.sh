#!/usr/bin/env sh
# shellcheck disable=SC2059
# help: test omni-runtime
# flags:
# --java | optional. tests java code only
# --cpp | optional. tests cpp code only

set -e

_info "Testing omni-runtime code"

FLAG_java="${FLAG_java:-False}"
FLAG_cpp="${FLAG_cpp:-False}"

if [ "$FLAG_java" = "False" ] && [ "$FLAG_cpp" = "False" ]; then
  _debug "Testing both java and cpp code"
  FLAG_java=True
  FLAG_cpp=True
fi

buildFolder="build"
libFile="$buildFolder/src/libomni_runtime.so"
if [ ! -f "$libFile" ]; then
  _error "Can't find omni-runtime libraries, try compiling omni-runtime first with build-omni script"
  exit 1
fi
cd "$buildFolder"
make install
cd -

export LD_LIBRARY_PATH=/opt/lib:/opt/lib/ir:/usr/lib:/usr/local/lib

if [ "$FLAG_cpp" = "True" ]; then
  testFile="$buildFolder/test/omtest"
  if [ ! -f "$testFile" ]; then
    _error "Can't find omni-runtime test file, try compiling omni-runtime first with build-omni script"
    exit 1
  fi

  ./"$testFile"
fi


if [ "$FLAG_java" = "True" ]; then
  proxyfile="$HOME/.m2/settings.xml"
  if [ -f "$proxyfile" ]; then
    sed -i "s/<port>/<port>$MVN_SETTINGS_PORT/g" "$proxyfile"
  fi
  cd bindings/java
  mvn clean test
  rm -rf target
  cd -
fi

_info "Successfully tested omni-runtime"
