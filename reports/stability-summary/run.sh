set -e

sudo apt-get -y install postgresql-client python-psycopg2 jq
sudo easy_install boto3

# Fetch metadata
META=net-mozaws-prod-us-west-2-pipeline-metadata
# Get metadata:
aws s3 cp s3://$META/sources.json ./

JOBNAME=telemetry-executive-summary-db
META_PREFIX=$(jq -r ".[\"${JOBNAME}\"][\"metadata_prefix\"]" < sources.json)

aws s3 cp s3://$META/$META_PREFIX/read/credentials.json ./

DB_HOST=$(jq -r '.["host"]' < credentials.json)
DB_NAME=$(jq -r '.["db_name"]' < credentials.json)
DB_USER=$(jq -r '.["username"]' < credentials.json)
DB_PW=$(jq -r '.["password"]' < credentials.json)

CONNECTION_STRING="host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PW"

echo "running rollup.py"
python rollup.py -d "$CONNECTION_STRING"
