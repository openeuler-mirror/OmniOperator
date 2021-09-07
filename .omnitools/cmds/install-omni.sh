#!/usr/bin/env sh
# shellcheck disable=SC2059
# help: install omni-runtime

set -e

if [ ! -f "build/opt/lib/libomni_runtime.so" ]; then
  _error "Can't find files to install, try compiling omni-runtime first with build-omni script"
  exit 1
fi

_info "Installing omni-runtime libraries"

cp -r build/opt /

_info "Finished installing omni-runtime libraries"
