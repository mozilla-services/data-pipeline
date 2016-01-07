sudo apt-get -y install postgresql-client
sudo apt-get -y install python-psycopg2
sudo easy_install boto3
python rollup.py -d "$(cat connection-details.txt)"
