#!/bin/bash

# Install dependencies: heka, hindsight, whatever
sudo apt-get install lua5.1
sudo dpkg -i luasandbox-0.10.2-Linux-core.deb

OUTPUT=output
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi

S3OUTPUT=s3output
if [ ! -d "$S3OUTPUT" ]; then
    mkdir -p "$S3OUTPUT"
fi

# Install dependencies: heka, hindsight, whatever
wget http://people.mozilla.org/~mtrinkala/heka-20151124-0_11_0-linux-amd64.tar.gz -O heka.tar.gz
tar xzf heka.tar.gz
 
# rename the dir to make it easier to refer to
mv heka-* heka
cp snappy.so heka/share/heka/lua_modules/

cd derived_streams 
# If we have an argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

# Update schema with target:
sed -r "s/__TARGET__/$TARGET/" schema_template.json > schema.json

# Run code:
../heka/bin/heka-s3list -schema schema.json -bucket='net-mozaws-prod-us-west-2-pipeline-data' -bucket-prefix='telemetry-release' > list.txt
lua splitter.lua
../hindsight/bin/hindsight_cli hindsight.cfg 7
