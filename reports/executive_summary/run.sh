#!/bin/bash

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

sed -r "s/__TARGET__/$TARGET/" schema_template.exec.json > schema.exec.json

echo "Fetching previous state..."
aws s3 sync s3://telemetry-private-analysis-2/executive-report-v4/data/sandbox_preservation/ "$OUTPUT/sandbox_preservation/"

wget http://people.mozilla.org/~mreid/heka-minimal.tar.gz
tar xzf heka-minimal.tar.gz

for f in $(ls $OUTPUT/sandbox_preservation/Firefox*.data.gz); do
    # Back up previous state
    BACKUP=$(echo "$f" | sed -r "s/[.]data[.]/.data.prev./")
    cp "$f" "$BACKUP"
    gunzip "$f"
done

# Run the report on $TARGET
heka-0_10_0-linux-amd64/bin/hekad -config exec.toml

echo "Compressing output"
gzip $OUTPUT/sandbox_preservation/Firefox*.data
echo "Done!"

echo "Outputting to demo dashboard"
aws s3 cp "$OUTPUT/dashboard/data/FirefoxMonthly.month.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_monthly_data.csv --grants full=emailaddress=mmayo@mozilla.com,emailaddress=cloudservices-aws-dev@mozilla.com,emailaddress=svcops-aws-dev@mozilla.com,emailaddress=svcops-aws-prod@mozilla.com
aws s3 cp "$OUTPUT/dashboard/data/FirefoxWeekly.week.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_weekly_data.csv --grants full=emailaddress=mmayo@mozilla.com,emailaddress=cloudservices-aws-dev@mozilla.com,emailaddress=svcops-aws-dev@mozilla.com,emailaddress=svcops-aws-prod@mozilla.com
aws s3 cp "$OUTPUT/dashboard/data/FirefoxDaily.day.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_daily_data.csv --grants full=emailaddress=mmayo@mozilla.com,emailaddress=cloudservices-aws-dev@mozilla.com,emailaddress=svcops-aws-dev@mozilla.com,emailaddress=svcops-aws-prod@mozilla.com
echo "Done!"
