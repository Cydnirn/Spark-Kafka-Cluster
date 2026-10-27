#!/bin/sh
# Seed the notebook workspace with the examples on first start, without ever
# overwriting work in progress. Then hand over to whatever the image's CMD is.
set -e

if [ -d /opt/examples ]; then
  for f in /opt/examples/*.ipynb; do
    [ -e "$f" ] || continue
    cp -n "$f" /opt/notebooks/ 2>/dev/null || true
  done
fi

exec "$@"
