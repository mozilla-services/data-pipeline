#!/bin/bash

# Exit on error:
set -o errexit

# Machine config:
# sudo yum install -y git hg golang cmake rpmdevtools

BASE=$(pwd)

cd ../
if [ ! -d heka ]; then
    # Fetch a fresh heka clone
    git clone https://github.com/mozilla-services/heka
fi

cd heka

echo "Patching for larger message size"
patch message/message.go < $BASE/heka/patches/0001-Increase-message-size-limit-from-64KB-to-8MB.patch

echo "Patching to build `heka-export` cmd"
patch CMakeLists.txt < $BASE/heka/patches/0002-Add-cmdline-tool-for-uploading-to-S3.patch

# TODO: do this using cmake externals instead of shell-fu.
echo "Installing source files for `heka-export` cmd"
cp -R $BASE/heka/cmd/heka-export ./cmd/

echo "Adding external plugin for s3splitfile output"
echo 'add_external_plugin(git https://github.com/mreid-moz/data-pipeline master heka/plugins/s3splitfile __ignore_root)' >> cmake/plugin_loader.cmake

source build.sh

# Build RPM
make package
