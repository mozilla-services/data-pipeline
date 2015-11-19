#!/bin/bash

# Exit on error:
set -o errexit

pushd .
# Machine config:
# sudo yum install -y git hg golang cmake rpmdevtools GeoIP-devel rpmrebuild

BUILD_BRANCH=$1
if [ -z "$BUILD_BRANCH" ]; then
    BUILD_BRANCH=master
fi

UNAME=$(uname)
MIN_GEOIP_VER=1.6.3
case $UNAME in
Darwin)
    echo "Ensuring we have the latest geoip for OSX..."
    MIN_GEOIP_VER=1.6.3
    if [ ! -z "$(which brew)" ]; then
        OSX_GEO_VER=$(brew info geoip | grep "geoip:" | sed -r "s/^.* ([0-9]+[.][0-9]+[.][0-9]+) .*$/\1/")
        if [ ! -z "$OSX_GEO_VER" ]; then
            X=$(printf '%s\n%s' "$MIN_GEOIP_VER" "$OSX_GEO_VER" | sort -V | head -1)
            if [ "$X" != "$MIN_GEOIP_VER" ]; then
                echo "Existing geoip version ($OSX_GEO_VER) is too old (we need at least $MIN_GEOIP_VER). Upgrading..."
                brew upgrade geoip
            else
                echo "Looks like geoip version $OSX_GEO_VER is good (>= $MIN_GEOIP_VER)"
            fi
        fi
    fi
    ;;
*)
    # Don't know how to upgrade libs in general...
    ;;
esac

BASE=$(pwd)
# To override the location of the Lua headers, use something like
#   export LUA_INCLUDE_PATH=/usr/include/lua5.1
if [ -z "$LUA_INCLUDE_PATH" ]; then
    # Default to the headers included with heka.
    LUA_INCLUDE_PATH=$BASE/build/heka/build/heka/include/luasandbox
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
# pin the Heka version
git fetch
git checkout 6f4b61a11cd01f1548dbd7dc8ebd0ed1cf9edbba

if [ ! -f "patches_applied" ]; then
    touch patches_applied

    echo "Patching to build 'heka-export' cmd"
    patch CMakeLists.txt < $BASE/heka/patches/0002-Add-cmdline-tool-for-uploading-to-S3.patch

    echo "Patching to build 'heka-s3list' and 'heka-s3cat'"
    patch CMakeLists.txt < $BASE/heka/patches/0003-Add-more-cmds.patch

    echo "Adding external plugin for s3splitfile output"
    echo "add_external_plugin(git https://github.com/mozilla-services/data-pipeline/s3splitfile :local)" >> cmake/plugin_loader.cmake
    echo "add_external_plugin(git https://github.com/mozilla-services/data-pipeline/snap :local)" >> cmake/plugin_loader.cmake

    echo "Adding external plugin for golang-lru output"
    echo "add_external_plugin(git https://github.com/mreid-moz/golang-lru acc5bd27065280640fa0a79a973076c6abaccec8)" >> cmake/plugin_loader.cmake

    echo "Adding kafka consumer group input"
    echo "add_external_plugin(git https://github.com/mozilla-services/data-pipeline/kafkaconsumergroup :local)" >> cmake/plugin_loader.cmake
    # Use known revisions (current "master" as of 2015-11-13)
    echo "add_external_plugin(git https://github.com/wvanbergen/kafka f56c2422fa9e4739981ed686ac5133d10e41b2d5 __ignore_root)" >> cmake/plugin_loader.cmake
    echo "add_external_plugin(git https://github.com/wvanbergen/kazoo-go 549793569f2034e3779263fb0d6b16db94186607)" >> cmake/plugin_loader.cmake
    echo "add_external_plugin(git https://github.com/samuel/go-zookeeper 218e9c81c0dd8b3b18172b2bbfad92cc7d6db55f __ignore_root)" >> cmake/plugin_loader.cmake

    echo "Fixup Hindsight plugin installation"
    echo "install(DIRECTORY $BASE/hindsight/modules/ DESTINATION share/heka/lua_modules)" >> cmake/plugin_loader.cmake
    echo "install(DIRECTORY $BASE/hindsight/io_modules/ DESTINATION share/heka/lua_io_modules)" >> cmake/plugin_loader.cmake
    echo "install(DIRECTORY $BASE/hindsight/output/ DESTINATION share/heka/lua_outputs)" >> cmake/plugin_loader.cmake
fi

# TODO: do this using cmake externals instead of shell-fu.
echo "Installing/updating source files for extra cmds"
cp -R $BASE/heka/cmd/heka-export ./cmd/
cp -R $BASE/heka/cmd/heka-s3list ./cmd/
cp -R $BASE/heka/cmd/heka-s3cat ./cmd/

echo 'Installing/updating lua filters/modules/decoders/encoders'
rsync -vr $BASE/heka/sandbox/ ./sandbox/lua/

echo 'Updating plugins with local changes'
mkdir -p $BASE/build/heka/externals
rsync -av $BASE/heka/plugins/ $BASE/build/heka/externals/

source build.sh

echo 'Installing lua-geoip libs'
cd $BASE/build
if [ ! -d lua-geoip ]; then
    # Fetch the lua geoip lib
    git clone https://github.com/agladysh/lua-geoip.git
fi
cd lua-geoip

# Use a known revision (current "master" with stderr fix Sept 11)
git checkout a07d261d8a2c7ff854fe6cd72cb8c2e16ec638ff

# from 'make.sh'
gcc -O2 -fPIC -I${LUA_INCLUDE_PATH} -c src/*.c -Isrc/ -Wall --pedantic -Werror --std=c99 -fms-extensions

SO_FLAGS="-shared -fPIC -s -O2"
case $UNAME in
Darwin)
    echo "Looks like OSX"
    SO_FLAGS="-bundle -undefined dynamic_lookup -fPIC -O2"
    ;;
*)
    echo "Looks like Linux"
    # Default flags apply.
    ;;
esac

HEKA_MODS=$BASE/build/heka/build/heka/lib/luasandbox/modules
mkdir -p $HEKA_MODS/geoip
gcc $SO_FLAGS database.o city.o -l GeoIP -o $HEKA_MODS/geoip/city.so
gcc $SO_FLAGS database.o country.o -l GeoIP -o $HEKA_MODS/geoip/country.so
gcc $SO_FLAGS database.o lua-geoip.o -l GeoIP -o $HEKA_MODS/geoip.so

echo 'Installing lua-gzip lib'
cd $BASE/build
if [ ! -d lua-gzip ]; then
    git clone https://github.com/vincasmiliunas/lua-gzip.git
fi
cd lua-gzip

# Use a known revision (current "master" as of 2015-02-12)
git checkout fe9853ea561d0957a18eb3c4970ca249c0325d84

gcc -I${LUA_INCLUDE_PATH} $SO_FLAGS lua-gzip.c -lz -o $HEKA_MODS/gzip.so

echo 'Installing lua-openssl lib'
cd $BASE/build
if [ ! -d lua-openssl ]; then
    git clone https://github.com/zhaozg/lua-openssl.git
fi
cd lua-openssl

# Use a known revision (current "master" as of 2015-10-28)
git checkout c85eb39a0fa5f45a778b09672773f95c2609c2d0

LUA_OPENSSL_SRC="src/asn1.c src/auxiliar.c src/bio.c src/cipher.c src/cms.c src/compat.c src/crl.c src/csr.c src/dh.c src/digest.c src/dsa.c
src/ec.c src/engine.c src/hmac.c src/lbn.c src/lhash.c src/misc.c src/ocsp.c src/openssl.c src/ots.c src/pkcs12.c src/pkcs7.c
src/pkey.c src/rsa.c src/ssl.c src/th-lock.c src/util.c src/x509.c src/xattrs.c src/xexts.c src/xname.c src/xstore.c src/xalgor.c src/callback.c"

gcc -DPTHREADS -I${LUA_INCLUDE_PATH} -Ideps $SO_FLAGS $LUA_OPENSSL_SRC -lssl -lcrypto -lrt -ldl -o $HEKA_MODS/openssl.so

HEKA_IO_MODS=$BASE/build/heka/build/heka/lib/luasandbox/io_modules
mkdir -p $HEKA_IO_MODS/luasql
echo 'Installing luasql-postgresql lib'
cd $BASE/build
if [ ! -d luasql-postgresql ]; then
    git clone https://github.com/LuaDist/luasql-postgresql.git
fi
cd luasql-postgresql

# Use a known revision (current "master" 2013-02-18)
git checkout 29a3aa1964aeac93323ec5d1446ac7d32ec700df

gcc -I/usr/include/postgresql -I${LUA_INCLUDE_PATH} $SO_FLAGS src/ls_postgres.c src/luasql.c -lpq -o $HEKA_IO_MODS/luasql/postgres.so

echo 'Installing lua_hash lib'
cd $BASE
# Build a hash module with the zlib checksum functions
gcc -I${LUA_INCLUDE_PATH} $SO_FLAGS heka/plugins/hash/lua_hash.c -lz -o $HEKA_MODS/hash.so

echo 'Installing fx libs'
mkdir -p $HEKA_MODS/fx
cd $BASE
gcc -I${LUA_INCLUDE_PATH} $SO_FLAGS --std=c99 heka/plugins/fx/executive_report.c heka/plugins/fx/xxhash.c heka/plugins/fx/common.c -o $HEKA_MODS/fx/executive_report.so

cd $BASE/build/heka/build

case $UNAME in
Darwin)
    # Don't bother trying to build a package on OSX
    make

    # Try setting the LD path (just in case this script was sourced)
    export DYLD_LIBRARY_PATH=build/heka/build/heka/lib
    echo "If you see an error like:"
    echo "    dyld: Library not loaded: libluasandbox.0.dylib"
    echo "You must first set the LD path:"
    echo "    export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH"
    ;;
*)
    # Build RPM
    make package
    export LD_LIBRARY_PATH=build/heka/build/heka/lib
    echo "If you see an error about libluasandbox, you must first set the LD path:"
    echo "    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
    ;;
esac
if hash rpmrebuild 2>/dev/null; then
    echo "Rebuilding RPM with date iteration and svc suffix"
    rpmrebuild -d . --release=0.$(date +%Y%m%d)svc -p -n heka-*-linux-amd64.rpm
fi
popd
