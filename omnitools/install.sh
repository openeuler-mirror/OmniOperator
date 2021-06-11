#!/usr/bin/env sh

set -eu


# note: this just copies the repo to a defined location then symlinks omnitools.sh
#       to 'omnitools' (ideally in a PATH location)


FORCE_YES="${FORCE_YES:-no}"
SOURCE_DIR="${SOURCE_DIR:-/etc/omnitools/}"
BIN_DIR="${BIN_DIR:-/usr/local/bin/}"

echo "installing to $SOURCE_DIR ..."

preflight() {
  if [ -d "$SOURCE_DIR" ]; then
    if [ "$FORCE_YES" = "no" ]; then
      # ask the user to manage this dir instead of overwriting (cheap)
      >&2 echo "error: install directory $SOURCE_DIR exists. please remove it"
      exit 1
    else
      echo "warning! install directory $SOURCE_DIR exists!"
    fi
  fi
}

preflight  # run checks
mkdir -p "$SOURCE_DIR"
cp -r ./* "$SOURCE_DIR"
ln -sf "${SOURCE_DIR}omnitools.sh" "${BIN_DIR}omnitools"
sh proxy/set_docker_proxy.sh
echo "'omnitools' installed to $BIN_DIR"
