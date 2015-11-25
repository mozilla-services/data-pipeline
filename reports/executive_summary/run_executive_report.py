#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Run the executive report using the Executive Summary data in Redshift

## SETUP
# sudo apt-get install libpq-dev python-dev
# sudo pip install psycopg2

import sys
import json
import argparse
import psycopg2
from psycopg2.extras import DictCursor
from boto.ses import connect_to_region as ses_connect
from datetime import datetime, timedelta

# TODO: fetch db credentials from S3 or something?

# TODO: monthly mode?
def get_create_temp_tables(start_date):
    tables = []
    for i in range(7):
        tables.append("SELECT * FROM executive_summary_{}".format(datetime.strftime(start_date + timedelta(i), "%Y%m%d")))

    sql = "CREATE VIEW this_week AS {}; ".format(" UNION ALL ".join(tables))
    tables = []
    for i in range(7):
        tables.append("SELECT * FROM executive_summary_{}".format(datetime.strftime(start_date + timedelta(-1 * (7 - i)), "%Y%m%d")))

    sql += "CREATE VIEW last_week AS {}; ".format(" UNION ALL ".join(tables))

    # Get latest values for each clientid:
    sql += """
CREATE VIEW client_values AS
SELECT clientid, country, channel, os, new_client, default_client FROM (
 SELECT
  clientid, country, channel, os,
  CASE WHEN profilecreationtimestamp >= '{}'::DATE THEN 1 ELSE 0 END as new_client,
  CASE WHEN "default" THEN 1 ELSE 0 END as default_client,
  -- Do not use "rank()" because it gives ties all the same value, so we end
  -- up with many "1" values if we order by country, channel, geo (hence over-
  -- counting the inactives)
  row_number() OVER (
   -- Use the most recently observed values:
   PARTITION BY clientid ORDER BY "timestamp" desc
  ) AS clientid_rank
 FROM this_week
) v
WHERE v.clientid_rank = 1;""".format(datetime.strftime(start_date, "%Y-%m-%d"))

    return sql


def get_easy_aggregates(start_date):
    # The .format(...) is a huge no-no, so we should pass in the date as a param instead.
    return """SELECT
 country AS geo, channel, os, '{}'::DATE AS "date",
 sum(hours) AS hours,
 sum(case when doctype = 'crash' then 1 else 0 end) AS crashes,
 sum(google) AS google,
 sum(bing) AS bing,
 sum(yahoo) AS yahoo,
 sum(other) AS other
FROM this_week GROUP BY 1, 2, 3, 4;""".format(datetime.strftime(start_date, "%Y-%m-%d"))


def get_client_aggregates(start_date):
    return """SELECT
 country AS geo, channel, os, '{}'::DATE as "date",
 COUNT(*) AS actives,
 SUM(new_client) AS new_clients,
 SUM(default_client) AS "default"
FROM client_values
GROUP BY 1, 2, 3, 4;""".format(datetime.strftime(start_date, "%Y-%m-%d"))
    pass

def get_inactives(start_date):
    return """SELECT country AS geo, channel, os, '{}'::DATE as "date", COUNT(*) AS inactives FROM (
 SELECT * FROM (
  SELECT
   clientid,
   country,
   channel,
   os,
   -- Do not use "rank()" because it gives ties all the same value, so we end
   -- up with many "1" values if we order by country, channel, geo (hence over-
   -- counting the inactives)
   row_number() OVER (
    -- Use the most recently observed values:
    PARTITION BY clientid ORDER BY "timestamp" desc
   ) AS clientid_rank
  FROM last_week WHERE clientid IN (
   SELECT clientid FROM last_week EXCEPT SELECT clientid FROM this_week
  )
 ) AS ranked
 WHERE ranked.clientid_rank = 1
) t GROUP BY 1, 2, 3, 4;""".format(datetime.strftime(start_date, "%Y-%m-%d"))

def get_five_of_seven(start_date):
    return """SELECT country as geo, channel, os, '{}' as "date",
 sum(
  CASE WHEN num_days >= 5 THEN 1 ELSE 0 END
 ) as five_of_seven
FROM (
 SELECT
  clientid, country, channel, os,
  -- Number of days on which we received submissions from this client.
  -- TODO: should we use activity date?
  count(distinct "timestamp"::date) as num_days
 FROM this_week GROUP BY 1, 2, 3, 4
) v;""".format(datetime.strftime(start_date, "%Y-%m-%d"))

def get_row_key(row):
    return (row["geo"], row["channel"], row["os"], row["date"])


ACTIVES = 0
HOURS = 1
INACTIVES = 2
NEW_RECORDS = 3
FIVE_OF_SEVEN = 4
TOTAL_RECORDS = 5
CRASHES = 6
DEFAULT = 7
GOOGLE = 8
BING = 9
YAHOO = 10
OTHER = 11

def main():
    parser = argparse.ArgumentParser(description="Run Executive Report")
    parser.add_argument("--report-start", help="Start day of the reporting period (YYYYMMDD)", required=True)
    parser.add_argument("--mode", help="Report mode: weekly or monthly", default="monthly")
    parser.add_argument("--db-url", help="Database URL to connect to", required=True)
    # parser.add_argument("--from-email", help="Email 'from:' address", required=True)
    # parser.add_argument("--to-email", help="Email 'to:' address (multiple allowed)", action="append", required=True)
    parser.add_argument("--dry-run", help="Print out what would happen instead of sending alert email", action="store_true")
    parser.add_argument("--verbose", help="Print all the messages", action="store_true")
    args = parser.parse_args()

    try:
        report_start_date = datetime.strptime(args.report_start, "%Y%m%d")
    except Exception as e:
        print "Error parsing report start date from '{}'. Should be in YYYYMMDD form. Error: {}".format(args.report_start, e)
        return 2

    if args.mode != 'weekly' and args.mode != 'monthly':
        print "Unknown run mode '{}'. Should be either 'weekly' or 'monthly'".format(args.mode)
        return 2

    exit_code = 0
    report = {}

    if args.verbose:
        print >> sys.stderr, "Preparing to generate weekly report for {}".format(args.report_start)

    if args.dry_run:
        print >> sys.stderr, "-- Dry run mode. Printing queries only."
        print get_create_temp_tables(report_start_date)
        print get_easy_aggregates(report_start_date)
        print get_client_aggregates(report_start_date)
        print get_inactives(report_start_date)
        print get_five_of_seven(report_start_date)
        return exit_code


    with psycopg2.connect(args.db_url) as conn:
        # with conn.cursor('exec_report{}'.format(args.report_start), cursor_factory=DictCursor) as cursor:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            if args.verbose:
                print >> sys.stderr, "Creating temp tables..."
            cursor.execute(get_create_temp_tables(report_start_date))

            if args.verbose:
                print >> sys.stderr, "Generating simple aggregates..."
            cursor.execute(get_easy_aggregates(report_start_date))
            #geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other
            for row in cursor:
                # Key fields
                k = get_row_key(row)
                v = [0,row["hours"],0,0,0,0,row["crashes"],0,row["google"],row["bing"],row["yahoo"],row["other"]]
                report[k] = v

            if args.verbose:
                print >> sys.stderr, "Generating per-client aggregates..."
            cursor.execute(get_client_aggregates())
            for row in cursor:
                k = get_row_key(row)
                v = report.get(k)
                if v is None:
                    v = [0,0,0,0,0,0,0,0,0,0,0,0]
                v[ACTIVES] = row["actives"]
                v[NEW_RECORDS] = row["new_clients"]
                v[DEFAULT] = row["default"]
                report[k] = v

            if args.verbose:
                print >> sys.stderr, "Generating inactives..."
            cursor.execute(get_inactives(report_start_date))
            for row in cursor:
                k = get_row_key(row)
                v = report.get(k)
                if v is None:
                    v = [0,0,0,0,0,0,0,0,0,0,0,0]
                v[INACTIVES] = row["inactives"]
                report[k] = v

            if args.verbose:
                print >> sys.stderr, "Generating five-of-seven..."
            cursor.execute(get_five_of_seven(report_start_date))
            for row in cursor:
                k = get_row_key(row)
                v = report.get(k)
                if v is None:
                    v = [0,0,0,0,0,0,0,0,0,0,0,0]
                v[FIVE_OF_SEVEN] = row["five_of_seven"]
                report[k] = v
            if args.verbose:
                print >> sys.stderr, "All done with database-fu."

    if args.verbose:
        print >> sys.stderr, "Outputting data."
    for k, v in report.iteritems():
        print "{},{}".format(",".join(k), ",".format(v))

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
