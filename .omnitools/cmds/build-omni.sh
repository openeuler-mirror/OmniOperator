#!/usr/bin/env sh
# shellcheck disable=SC2059
# help: build omni-runtime
# flags:
# --java | optional. builds java code only
# --cpp | optional. builds cpp code only
# --debug_low | optional. builds cpp code with debug low symbols
# --debug_high | optional. builds cpp code with debug high symbols
# --debug_op | optional. builds cpp code with debug native operator symbols

set -e

_info "Building omni-runtime code"

FLAG_java="${FLAG_java:-False}"
FLAG_cpp="${FLAG_cpp:-False}"
FLAG_debug_low="${FLAG_debug_low:-False}"
FLAG_debug_high="${FLAG_debug_high:-False}"
FLAG_debug_op="${FLAG_debug_op:-False}"

if [ "$FLAG_java" = "False" ] && [ "$FLAG_cpp" = "False" ]; then
  _debug "Compiling both java and cpp code"
  FLAG_java=True
  FLAG_cpp=True
fi

buildFolder="$PWD/build"
if [ ! -d "$buildFolder" ]; then
  mkdir -p "$buildFolder"
fi

export LD_LIBRARY_PATH=/opt/lib:/opt/lib/ir:/usr/lib:/usr/local/lib

if [ "$FLAG_java" = "True" ]; then
  proxyfile="$HOME/.m2/settings.xml"
  if [ -f "$proxyfile" ]; then
    sed -i "s/<port>/<port>$MVN_SETTINGS_PORT/g" "$proxyfile"
  fi
  cd bindings/java
  mvn clean -T 1C install -DskipTests
  cd -
  mv bindings/java/target "$buildFolder"
fi

if [ "$FLAG_cpp" = "True" ]; then
  cd "$buildFolder"
  if [ "$FLAG_debug_low" = "True" ]; then
    _debug "Compiling with debug low symbols"
    cmake ../core -DDEBUG_LEVEL_LOW=ON
  elif [ "$FLAG_debug_high" = "True" ]; then
    _debug "Compiling with debug low symbols"
    cmake ../core -DDEBUG_LEVEL_HIGH=ON
  elif [ "$FLAG_debug_op" = "True" ]; then
    _debug "Compiling with debug native operator symbols"
    cmake ../core -DDEBUG_OPERATOR=ON
  else
    _debug "Compiling release version"
    cmake ../core
  fi
  make clean
  make -j4
fi

_info "Compiled omni-runtime in $buildFolder"
