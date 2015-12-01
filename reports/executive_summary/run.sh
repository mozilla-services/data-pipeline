#!/bin/bash

USAGE="Usage: bash $0 {monthly|weekly} [report_start_yyyymmdd]\nIf not specified, report start defaults to the period ending yesterday"
OUTPUT=output
TODAY=$(date +%Y%m%d)
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi

# First argument is "mode". It is required.
MODE=$1
if [ "$MODE" != "weekly" -a "$MODE" != "monthly" ]; then
    echo "Error: specify 'weekly' or 'monthly' report mode."
    echo -e $USAGE
    exit 1
fi

# If we have an argument, process that day.
TARGET=$2
if [ -z "$TARGET" ]; then
    # Default to processing the report period ending "yesterday"
    if [ "$MODE" = "weekly" ]; then
        TARGET=$(date -d '1 week ago - 1 day' +%Y%m%d)
    else
        TARGET=$(date -d '1 month ago - 1 day' +%Y%m%d)
    fi
fi

echo "Running $MODE report for period starting on $TARGET"

# Make sure we have 'jq' and other prereqs
export DEBIAN_FRONTEND=noninteractive; sudo apt-get --yes --force-yes install jq libpq-dev python-dev
sudo pip install psycopg2

# Fetch db connection details
## TODO: add this info to sources.json
aws s3 cp s3://net-mozaws-prod-us-west-2-pipeline-metadata/sources.json ./

# Get read-only conn string out.
# Code expects a URL of the form:
#   postgresql://username:password@hostname:port/dbname
DB_URL=$(jq -r '.["telemetry-executive-summary-db"].db_url' < sources.json)

CURRENT="$OUTPUT/executive_report.${MODE}.${TARGET}.csv"
time python run_executive_report.py \
        --verbose \
        --db-url "$DB_URL" \
        --report-start $TARGET \
        --mode $MODE > "$CURRENT"

OVERALL="v4-${MODE}.csv"
echo "Fetching previous state from $OVERALL..."
aws s3 cp "s3://net-mozaws-prod-metrics-data/firefox-executive-dashboard/$OVERALL" ./

if [ -s "$OVERALL" ]; then
    echo "Backing up previous state"
    # If we have an existing file, back it up.
    cp "$OVERALL" "$OUTPUT/${OVERALL}.pre_${TARGET}"
    gzip "$OUTPUT/${OVERALL}.pre_${TARGET}"
else
    echo "No previous state found, starting fresh"
    # If we don't have a previous state, add the header line from this run.
    head -n 1 "$CURRENT" > "$OVERALL"
fi

echo "Appending current date to overall state (minus header)"
# We should probably error if the header doesn't match the overall header...
tail -n +2 "$CURRENT" >> "$OVERALL"

# Run the cleanup script from:
#  https://github.com/mozilla/firefox-executive-dashboard/blob/master/data/reformat_v4.py
python reformat_v4.py --file "$OVERALL" --output "$OVERALL"

echo "Uploading updated state back to dashboard bucket"
# Upload the state back.
aws s3 cp "$OVERALL" "s3://net-mozaws-prod-metrics-data/firefox-executive-dashboard/"

# Then stick it in the output dir
mv "$OVERALL" "$OUTPUT/"

# And finally gzip it.
gzip "$OUTPUT/$OVERALL"
