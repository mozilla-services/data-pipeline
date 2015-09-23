#!/bin/bash
OUTPUT=output
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
wget http://people.mozilla.org/~mreid/heka-20150918-0_11_0-linux-amd64.tar.gz -O heka.tar.gz
tar xzf heka.tar.gz
mv heka-* heka

echo "Fetching previous state..."

aws s3 sync s3://telemetry-private-analysis/budget-report/data/sandbox_preservation/ "$OUTPUT/sandbox_preservation/"

sed -r "s/__TARGET__/$TARGET/" schema_template.json > schema.json
heka/bin/hekad -config budget.toml

# Push json to prod report bucket/path
DATA="$OUTPUT/dashboard/data/PipelineBudget.SubmissionSizesbychannelanddate.json"
aws s3 cp "$DATA" s3://net-mozaws-prod-metrics-data/telemetry-budget-dashboard/budget.json --acl bucket-owner-full-control

echo "Fetching budget targets"
aws s3 cp s3://net-mozaws-prod-us-west-2-pipeline-metadata/telemetry-2/budget_targets.json ./

# Alert if data for $TARGET exceeds expected volume.
ALERT_FROM=telemetry-alerts@mozilla.com
ALERT_TO=$ALERT_FROM
echo "Checking if we've exceeded targets"
python check_targets.py --day $TARGET \
                        --targets-file budget_targets.json \
                        --data-file "$DATA" \
                        --from-email $ALERT_FROM \
                        --to-email $ALERT_TO \
                        --verbose
