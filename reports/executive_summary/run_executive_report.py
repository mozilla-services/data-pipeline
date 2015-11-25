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

def this_week(start_date):
    tables = []
    for i in range(7):
        tables.append("SELECT * FROM executive_summary_{}".format(datetime.strftime(start_date + timedelta(i), "%Y%m%d")))

    return " UNION ALL ".join(tables)

def last_week(start_date):
    tables = []
    for i in range(7):
        tables.append("SELECT * FROM executive_summary_{}".format(datetime.strftime(start_date + timedelta(-1 * (7 - i)), "%Y%m%d")))

    return " UNION ALL ".join(tables)

def client_values(start_date, inline_date=False):
    template = """SELECT clientid, country, channel, os, new_client, default_client FROM (
 SELECT
  clientid, country, channel, os,
  CASE WHEN profilecreationtimestamp >= {start_date} THEN 1 ELSE 0 END as new_client,
  CASE WHEN "default" THEN 1 ELSE 0 END as default_client,
  -- Do not use "rank()" because it gives ties all the same value, so we end
  -- up with many "1" values if we order by country, channel, geo (hence over-
  -- counting the inactives)
  row_number() OVER (
   -- Use the most recently observed values:
   PARTITION BY clientid ORDER BY "timestamp" desc
  ) AS clientid_rank
 FROM ({this_week}) this_week
) v
WHERE v.clientid_rank = 1"""
    if inline_date:
        return template.format("'{}'::DATE".format(start_date=datetime.strftime(start_date, "%Y-%m-%d")), this_week=this_week(start_date))
    return template.format(start_date='%s', this_week=this_week(start_date))


def get_easy_aggregates(start_date, inline_date=False):
    template = """SELECT
 country AS geo, channel, os, {start_date} AS "date",
 sum(hours) AS hours,
 -- Count the number of crash documents
 sum(case when doctype = 'crash' then 1 else 0 end) AS crashes,
 sum(google) AS google,
 sum(bing) AS bing,
 sum(yahoo) AS yahoo,
 sum(other) AS other
FROM ({this_week}) this_week GROUP BY 1, 2, 3, 4"""
    date_param = '%s'
    if inline_date:
        date_param = "'{}'::DATE".format(datetime.strftime(start_date, "%Y-%m-%d"))
    return template.format(start_date=date_param, this_week=this_week(start_date))

def get_client_aggregates(start_date, inline_date=False):
    template = """SELECT
 country AS geo, channel, os, {start_date} as "date",
 COUNT(*) AS actives,
 SUM(new_client) AS new_clients,
 SUM(default_client) AS "default"
FROM ({client_values}) client_values
GROUP BY 1, 2, 3, 4;"""
    date_param = '%s'
    if inline_date:
        date_param = "'{}'::DATE".format(datetime.strftime(start_date, "%Y-%m-%d"))
    return template.format(start_date=date_param, client_values=client_values(start_date))

def get_inactives(start_date, inline_date=False):
    template = """SELECT country AS geo, channel, os, {start_date} as "date", COUNT(*) AS inactives FROM (
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
  FROM ({this_week}) this_week WHERE clientid IN (
   SELECT clientid FROM ({last_week}) last_week EXCEPT SELECT clientid FROM ({this_week}) this_week
  )
 ) AS ranked
 WHERE ranked.clientid_rank = 1
) t GROUP BY 1, 2, 3, 4;"""
    date_param = '%s'
    if inline_date:
        date_param = "'{}'::DATE".format(datetime.strftime(start_date, "%Y-%m-%d"))
    return template.format(start_date=date_param, last_week=last_week(start_date), this_week=this_week(start_date))

def get_five_of_seven(start_date, inline_date=False):
    template = """SELECT country as geo, channel, os, {start_date} as "date",
 sum(
  CASE WHEN num_days >= 5 THEN 1 ELSE 0 END
 ) as five_of_seven
FROM (
 SELECT
  clientid, country, channel, os,
  -- Number of days on which we received submissions from this client.
  count(distinct "timestamp"::date) as num_days
 FROM ({this_week}) this_week GROUP BY 1, 2, 3, 4
) v GROUP BY 1, 2, 3, 4;"""
    date_param = '%s'
    if inline_date:
        date_param = "'{}'::DATE".format(datetime.strftime(start_date, "%Y-%m-%d"))
    return template.format(start_date=date_param, this_week=this_week(start_date))

def get_row_key(row):
    return (row["geo"], row["channel"], row["os"], datetime.strftime(row["date"], "%Y-%m-%d"))

def nz(v):
    if v is None:
        return 0
    return v

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
    parser.add_argument("--report-start",  help="Start day of the reporting period (YYYYMMDD)", required=True)
    parser.add_argument("--mode",          help="Report mode: weekly or monthly", default="monthly")
    parser.add_argument("--db-url",        help="Database URL to connect to", required=True)
    parser.add_argument("--dry-run",       help="Print out what would happen instead of sending alert email", action="store_true")
    parser.add_argument("--skip-easy",     help="Skip computation of easy aggregates", action="store_true")
    parser.add_argument("--skip-client",   help="Skip computation of client aggregates", action="store_true")
    parser.add_argument("--skip-inactive", help="Skip computation of inactive count", action="store_true")
    parser.add_argument("--skip-fos",      help="Skip computation of five-of-seven count", action="store_true")
    parser.add_argument("--verbose",       help="Print all the messages", action="store_true")
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
        print get_easy_aggregates(report_start_date, inline_date=True)
        print get_client_aggregates(report_start_date, inline_date=True)
        print get_inactives(report_start_date, inline_date=True)
        print get_five_of_seven(report_start_date, inline_date=True)
        return exit_code

    sd = report_start_date.date()
    with psycopg2.connect(args.db_url) as conn:
        # with conn.cursor('exec_report{}'.format(args.report_start), cursor_factory=DictCursor) as cursor:
        with conn.cursor(cursor_factory=DictCursor) as cursor:

            if not args.skip_easy:
                if args.verbose:
                    print >> sys.stderr, "Generating simple aggregates..."
                cursor.execute(get_easy_aggregates(report_start_date), (sd,))
                for row in cursor:
                    # Key fields
                    k = get_row_key(row)
                    v = [0,nz(row["hours"]),0,0,0,0,nz(row["crashes"]),0,nz(row["google"]),nz(row["bing"]),nz(row["yahoo"]),nz(row["other"])]
                    report[k] = v

            if not args.skip_client:
                if args.verbose:
                    print >> sys.stderr, "Generating per-client aggregates..."
                cursor.execute(get_client_aggregates(report_start_date), (sd,sd))
                for row in cursor:
                    k = get_row_key(row)
                    v = report.get(k)
                    if v is None:
                        v = [0,0,0,0,0,0,0,0,0,0,0,0]
                    v[ACTIVES] = nz(row["actives"])
                    v[NEW_RECORDS] = nz(row["new_clients"])
                    v[DEFAULT] = nz(row["default"])
                    report[k] = v

            if not args.skip_inactive:
                if args.verbose:
                    print >> sys.stderr, "Generating inactives..."
                cursor.execute(get_inactives(report_start_date), (sd,))
                for row in cursor:
                    k = get_row_key(row)
                    v = report.get(k)
                    if v is None:
                        v = [0,0,0,0,0,0,0,0,0,0,0,0]
                    v[INACTIVES] = nz(row["inactives"])
                    report[k] = v

            if not args.skip_fos:
                if args.verbose:
                    print >> sys.stderr, "Generating five-of-seven..."
                cursor.execute(get_five_of_seven(report_start_date), (sd,))
                for row in cursor:
                    k = get_row_key(row)
                    v = report.get(k)
                    if v is None:
                        v = [0,0,0,0,0,0,0,0,0,0,0,0]
                    v[FIVE_OF_SEVEN] = nz(row["five_of_seven"])
                    report[k] = v

            if args.verbose:
                print >> sys.stderr, "All done with database-fu."

    if args.verbose:
        print >> sys.stderr, "Outputting data."

    print "geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other"
    for k, v in report.iteritems():
        s = [ "{}".format(kk) for kk in k ]
        for vv in v:
            s.append("{}".format(vv))
        print ",".join(s)

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
