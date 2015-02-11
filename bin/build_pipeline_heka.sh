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
# To override the location of the Lua headers, use something like
#   export LUA_INCLUDE_PATH=/usr/include/lua5.1
if [ -z "$LUA_INCLUDE_PATH" ]; then
    # Default to the headers included with heka.
    LUA_INCLUDE_PATH=$BASE/build/heka/build/heka/include
fi

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

echo 'Installing lua-geoip libs'
cd $BASE/build
if [ ! -d lua-geoip ]; then
    # Fetch the lua geoip lib
    git clone https://github.com/agladysh/lua-geoip.git
    cd lua-geoip
    # from 'make.sh'
    gcc -O2 -fPIC -I${LUA_INCLUDE_PATH} -c src/*.c -Isrc/ -Wall --pedantic -Werror --std=c99 -fms-extensions

    UNAME=$(uname)
    case $UNAME in
    Darwin)
        echo "Looks like OSX"
        gcc -bundle -undefined dynamic_lookup database.o city.o -o city.so
        gcc -bundle -undefined dynamic_lookup database.o country.o -o country.so
        gcc -bundle -undefined dynamic_lookup database.o lua-geoip.o -o geoip.so
        ;;
    *)
        echo "Looks like Linux"
        gcc -shared -fPIC database.o city.o -o city.so
        gcc -shared -fPIC database.o country.o -o country.so
        gcc -shared -fPIC database.o lua-geoip.o -o geoip.so
        ;;
    esac
    cd -
fi

cd $BASE/build/heka/build
cp $BASE/build/lua-geoip/geoip.so heka/modules/
mkdir -p heka/modules/geoip
cp $BASE/build/lua-geoip/c*.so heka/modules/geoip


# Build RPM
make package
if hash rpmrebuild 2>/dev/null; then
    echo "Rebuilding RPM with date iteration and svc suffix"
    rpmrebuild -d . --release=0.$(date +%Y%m%d)svc -p -n heka-*-linux-amd64.rpm
fi
