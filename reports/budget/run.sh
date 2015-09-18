#!/bin/bash

echo "ohai"

OUTPUT=output
TODAY=$(date +%Y%m%d)
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT/sandbox_preservation"
fi

# If we have an argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

# Install heka
wget http://people.mozilla.org/~mreid/heka-20150916-0_10_0-linux-amd64.tar.gz -O heka.tar.gz
tar xzf heka.tar.gz
mv heka-* heka

echo "Fetching previous state..."

aws s3 sync s3://telemetry-private-analysis/budget-report/data/sandbox_preservation/ "$OUTPUT/sandbox_preservation/"

sed -r "s/__TARGET__/$TARGET/" schema_template.json > schema.json
heka/bin/hekad -config budget.toml

# TODO: push json to prod report bucket/path
# aws s3 cp "$OUTPUT/dashboard/data/PipelineBudget.SubmissionSizesbychannelanddate.json" s3://bucket/path/to/budget.json

# TODO: alert if data for $TARGET exceeds expected volume.
