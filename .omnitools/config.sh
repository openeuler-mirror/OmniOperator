#!/usr/bin/env sh
# shellcheck disable=SC2034

set -e


# just some custom hash example - all of this is totally up to your needs
hash() {
  if command -v md5 > /dev/null; then
    find "$1" -type f -and -not -path "./.git/*" -exec md5 -q {} \; | md5
  elif command -v md5sum > /dev/null; then
    find "$1" -type f -and -not -path "./.git/*" -exec md5sum {} \; | awk '{ print $1 }' | md5sum | awk '{ print $1 }'
  else
    >&2 echo "[error] failed to hash. no md5 or md5sum found"
    exit 1
  fi
}

OMNITOOLS_IMAGE="${OMNITOOLS_IMAGE:-blopeur/omnitools:latest}"
# DOCKER_COMPOSE_FILE=
DOCKER_TAG="omni-runtime:$(hash "$PROJECT_ROOT")"
# DOCKER_FILE=
# DOCKER_CONTEXT=
# DOCKER_PROGRESS=
# HOME_DIR=