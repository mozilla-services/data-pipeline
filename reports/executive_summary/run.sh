#!/bin/bash

USAGE="Usage: bash $0 {monthly|weekly} [report_start_yyyymmdd]\nIf not specified, report start defaults to the most recent completed reporting period."
OUTPUT=output
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

# If we have a date argument, use that as the report start day.
TARGET=$2
if [ -z "$TARGET" ]; then
    # Default to processing the most recent completed reporting period.
    # For a week, that is the period ending on the most recent Saturday (and
    # starting on the prior Sunday)
    # For a month, it is the period starting on the first of the previous month.
    if [ "$MODE" = "weekly" ]; then
        # The Sunday of the previous complete week
        TARGET=$(date -d 'last sunday - 1 week' +%Y%m%d)
    else
        # The first day of the previous complete month
        TARGET=$(date -d '1 month ago' +%Y%m01)
    fi
fi

echo "Running $MODE report for period starting on $TARGET"

# Make sure we have 'jq' and other prereqs
export DEBIAN_FRONTEND=noninteractive; sudo apt-get --yes --force-yes install jq libpq-dev python-dev
sudo pip install psycopg2

# Fetch db connection details
META=net-mozaws-prod-us-west-2-pipeline-metadata
# Get metadata:
aws s3 cp s3://$META/sources.json ./
RC=$?
# Check if the copy succeeded. See:
#   http://docs.aws.amazon.com/cli/latest/topic/return-codes.html
if [ "$RC" -ne "0" ]; then
    echo "ERROR $RC fetching data sources."
    exit 2
fi
META_PREFIX=$(jq -r '.["telemetry-executive-summary-db"]["metadata_prefix"]' < sources.json)
# Get read-only credentials:
aws s3 cp s3://$META/$META_PREFIX/read/credentials.json ./
RC=$?
if [ "$RC" -ne "0" ]; then
    echo "ERROR $RC fetching read credentials."
    exit 3
fi

DB_HOST=$(jq -r '.host' < credentials.json)
DB_PORT=$(jq -r '.port' < credentials.json)
DB_NAME=$(jq -r '.db_name' < credentials.json)
DB_USER=$(jq -r '.username' < credentials.json)
DB_PASS=$(jq -r '.password' < credentials.json)

# Code expects a URL of the form:
#   postgresql://username:password@hostname:port/dbname
DB_URL="postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

CURRENT="$OUTPUT/executive_report.${MODE}.${TARGET}.csv"
time python run_executive_report.py \
        --verbose \
        --check-tables \
        --db-url "$DB_URL" \
        --report-start $TARGET \
        --mode $MODE > "$CURRENT"

RC=$?
if [ "$RC" -ne "0" ]; then
    echo "ERROR $RC running report."
    exit 5
fi

OVERALL="v4-${MODE}.csv"
DASHBOARD_S3="s3://net-mozaws-prod-metrics-data/firefox-dashboard"
echo "Fetching previous state from $OVERALL..."
aws s3 cp "$DASHBOARD_S3/$OVERALL" ./
RC=$?

if [ -s "$OVERALL" ]; then
    if  [ "$RC" -eq "0" ]; then
        echo "Backing up previous state"
        # If we have an existing file, back it up.
        cp "$OVERALL" "$OUTPUT/${OVERALL}.pre_${TARGET}"
        gzip "$OUTPUT/${OVERALL}.pre_${TARGET}"
        # TODO: Should we grep -v the TARGET date, replacing instead of potentially
        #       duplicating?
    else
        echo "ERROR $RC fetching previous state, aborting."
        exit 4
    fi
else
    echo "No previous state found, starting fresh"
    # If we don't have a previous state, add the header line from this run.
    head -n 1 "$CURRENT" > "$OVERALL"
fi

echo "Checking if the csv header is the same. Diffs:"
HEADER_DIFFS=$(diff <(head -n 1 $OVERALL) <(head -n 1 $CURRENT))
if [ ! -z "$HEADER_DIFFS" ]; then
    echo "WARNING: headers were different.  <<<old  >>>current"
    echo $HEADER_DIFFS
else
    echo "None. Headers match."
fi

echo "Appending current data to overall state (minus header)"
tail -n +2 "$CURRENT" >> "$OVERALL"

# Run the cleanup script
python reformat_v4.py --file "$OVERALL" --output "$OVERALL"

echo "Uploading updated state back to dashboard bucket"
# Upload the state back.
aws s3 cp "$OVERALL" "$DASHBOARD_S3/" --acl bucket-owner-full-control
RC=$?
if [ "$RC" -ne "0" ]; then
    echo "ERROR $RC re-uploading to dashbord bucket ($DASHBOARD_S3)."
fi

# Then stick it in the output dir
mv "$OVERALL" "$OUTPUT/"

# And finally gzip it.
gzip "$OUTPUT/$OVERALL"
