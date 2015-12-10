#!/bin/bash

VERSION=0.11

# Git doesn't keep empty dirs :(  Ensure that all necessary dirs are present.
mkdir -p derived_streams/run/analysis
mkdir -p derived_streams/run/input
mkdir -p derived_streams/run/output

tar czvf derived_streams-v4-${VERSION}.tar.gz derived_streams hindsight luasandbox-0.10.2-Linux-core.deb run.sh snappy.so
