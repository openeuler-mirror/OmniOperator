#!/usr/bin/env sh

set -eu


CONF_DIR="$HOME/.docker"
HTTP_PROXY="${HTTP_PROXY:-False}"
HTTPS_PROXY="${HTTPS_PROXY:-False}"
FTP_PROXY="${FTP_PROXY:-False}"
NO_PROXY="${NO_PROXY:-False}"

if [ "$HTTP_PROXY" = "False" ] && [ "$HTTPS_PROXY" = "False" ] && [ "$NO_PROXY" = "False" ] && [ "$FTP_PROXY" = "False" ]; then
  echo "warning: please note, the proxy is not set"
else
  if [ "$HTTP_PROXY" = "False" ]; then
    HTTP_PROXY=""
  fi
  if [ "$HTTPS_PROXY" = "False" ]; then
    HTTPS_PROXY=""
  fi
  if [ "$FTP_PROXY" = "False" ]; then
    FTP_PROXY=""
  fi
  if [ "$NO_PROXY" = "False" ]; then
    NO_PROXY=""
  fi
  printf "{\n  \"proxies\":\n  {\n    \"default\":\n    {\n      \"httpProxy\": \"$HTTP_PROXY\",\n      \"httpsProxy\": \"$HTTPS_PROXY\",\n      \"ftpProxy\": \"$FTP_PROXY\",\n      \"noProxy\": \"$NO_PROXY\"\n    }\n  }\n}" > config.json

  if [ ! -d "$CONF_DIR" ]; then
    mkdir "$CONF_DIR"
  fi
  mv config.json "$CONF_DIR"
  echo "the proxy config file is written to $CONF_DIR/config.json"
fi
