#!/usr/bin/env sh
# shellcheck disable=SC2059
# help: clean omni-runtime

set -e

_info "Cleaning omni-runtime libraries"

if [ -d "build" ]; then
  rm -rf "build"
fi

_info "Finished cleaning omni-runtime libraries"
