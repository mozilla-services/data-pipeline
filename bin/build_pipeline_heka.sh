#!/bin/bash

# Exit on error:
set -o errexit

# Machine config:
# sudo yum install -y git hg golang cmake rpmdevtools GeoIP-devel rpmrebuild

BUILD_BRANCH=$1
if [ -z "$BUILD_BRANCH" ]; then
    BUILD_BRANCH=master
fi

BASE=$(pwd)

if [ ! -d build ]; then
    mkdir build
fi

cd build
if [ ! -d heka ]; then
    # Fetch a fresh heka clone
    git clone https://github.com/mozilla-services/heka
fi

cd heka

if [ ! -f "patches_applied" ]; then
    touch patches_applied

    echo "Patching for larger message size"
    patch message/message.go < $BASE/heka/patches/0001-Increase-message-size-limit-from-64KB-to-8MB.patch

    echo "Patching to build 'heka-export' cmd"
    patch CMakeLists.txt < $BASE/heka/patches/0002-Add-cmdline-tool-for-uploading-to-S3.patch

    echo "Patching to build 'heka-s3list' and 'heka-s3cat'"
    patch CMakeLists.txt < $BASE/heka/patches/0003-Add-more-cmds.patch

    # TODO: do this using cmake externals instead of shell-fu.
    echo "Installing source files for extra cmds"
    cp -R $BASE/heka/cmd/heka-export ./cmd/
    cp -R $BASE/heka/cmd/heka-s3list ./cmd/
    cp -R $BASE/heka/cmd/heka-s3cat ./cmd/

    echo 'Installing lua filters/modules/decoders'
    rsync -vr $BASE/heka/sandbox/ ./sandbox/lua/

    echo "Adding external plugin for s3splitfile output"
    echo "add_external_plugin(git https://github.com/mozilla-services/data-pipeline $BUILD_BRANCH heka/plugins/s3splitfile __ignore_root)" >> cmake/plugin_loader.cmake
fi

source build.sh

# Build RPM
make package
if hash rpmrebuild 2>/dev/null; then
    echo "Rebuilding RPM with date iteration and svc suffix"
    rpmrebuild -d . --release=0.$(date +%Y%m%d)svc -p -n heka-*-linux-amd64.rpm
fi
