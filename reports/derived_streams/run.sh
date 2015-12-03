#!/bin/bash

# Install dependencies
sudo apt-get --yes install lua5.1 postgresql-client
sudo dpkg -i luasandbox-0.10.2-Linux-core.deb

OUTPUT=output
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi

S3OUTPUT=s3output
if [ ! -d "$S3OUTPUT" ]; then
    mkdir -p "$S3OUTPUT"
fi

# Install dependencies
wget http://people.mozilla.org/~mtrinkala/heka-20151124-0_11_0-linux-amd64.tar.gz -O heka.tar.gz
tar xzf heka.tar.gz

# Rename the dir to make it easier to refer to
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

# Fetch metadata
META=net-mozaws-prod-us-west-2-pipeline-metadata
# Get metadata:
aws s3 cp s3://$META/sources.json ./

# Get the Telemetry data location
BUCKET=$(jq -r '.["telemetry"].bucket' < sources.json)
PREFIX=$(jq -r '.["telemetry"].prefix' < sources.json)

# Run code:
../heka/bin/heka-s3list -schema schema.json -bucket="$BUCKET" -bucket-prefix="$PREFIX" > list.txt
lua splitter.lua
../hindsight/bin/hindsight_cli hindsight.cfg 7

echo "Loading data for $TARGET into Redshift..."

## TODO: We assume these are all in the same database. Should fetch credentials
#        for each table separately.
META_PREFIX=$(jq -r '.["telemetry-executive-summary-db"]["metadata_prefix"]' < sources.json)
# Get read-write credentials:
aws s3 cp s3://$META/$META_PREFIX/write/credentials.json ./

DB_HOST=$(jq -r '.host' < credentials.json)
DB_PORT=$(jq -r '.port' < credentials.json)
DB_NAME=$(jq -r '.db_name' < credentials.json)
DB_USER=$(jq -r '.username' < credentials.json)
DB_PASS=$(jq -r '.password' < credentials.json)

# Install these credentials for psql to use
#   See http://www.postgresql.org/docs/current/static/libpq-pgpass.html
echo "$DB_HOST:$DB_PORT:$DB_NAME:$DB_USER:$DB_PASS" >> ~/.pgpass
chmod 0600 ~/.pgpass

PQ="psql -U \"$DB_USER\" -h \"$DB_HOST\" -p $DB_PORT $DB_NAME"

# Fetch AWS credentials for IAM role
#  See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials
IAM_ROLE_NAME=$(curl http://169.254.169.254/latest/meta-data/iam/security-credentials/)
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/${IAM_ROLE_NAME} > aws_creds.json
AWS_KEY=$(jq -r '.AccessKeyId' < aws_creds.json)
AWS_SECRET_KEY=$(jq -r '.SecretAccessKey' < aws_creds.json)

CREDS="aws_access_key_id=$AWS_KEY;aws_secret_access_key=$AWS_SECRET_KEY"
for t in main crash executive; do
    NEW_TABLE="${t}_summary_${TARGET}"
    echo "Copying data for $NEW_TABLE..."
    $PQ -c "CREATE TABLE IF NOT EXISTS $NEW_TABLE (LIKE ${t}_summary including defaults);"
    $PQ -c "COPY $NEW_TABLE FROM 's3://telemetry-private-analysis-2/derived_streams/data/${NEW_TABLE}' CREDENTIALS '$CREDS' ACCEPTANYDATE TRUNCATECOLUMNS ESCAPE;"
done
for u in read_only read_write; do
    $PQ -c "GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO $u;"
done
