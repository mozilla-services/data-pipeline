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
from datetime import datetime, timedelta

def union(tables, fields=["*"]):
    global summary_tables
    good_tables = tables
    field_list = ",".join(fields)
    if summary_tables is not None:
        good_tables = []
        for table in tables:
            if table not in summary_tables:
                print >> sys.stderr, "WARNING: Skipping nonexistent table {}. Output will be incomplete".format(table)
                continue
            good_tables.append(table)
    return " UNION ALL ".join([ "SELECT {} FROM {}".format(field_list, t) for t in good_tables ])

def this_week(start_date, fields=["*"]):
    tables = []
    for i in range(7):
        tables.append("executive_summary_{}".format(datetime.strftime(start_date + timedelta(i), "%Y%m%d")))
    return union(tables, fields)

def this_month(start_date, fields=["*"]):
    tables = ["executive_summary_{}".format(datetime.strftime(start_date, "%Y%m%d"))]
    for i in range(1, 32):
        next_date = start_date + timedelta(i)
        # Stop (and exclude) when we iterate to the same day in the next month
        if next_date.day == start_date.day:
            break
        tables.append("executive_summary_{}".format(datetime.strftime(next_date, "%Y%m%d")))
    return union(tables, fields)

def last_week(start_date, fields=["*"]):
    tables = []
    for i in range(7):
        tables.append("executive_summary_{}".format(datetime.strftime(start_date + timedelta(-1 * (7 - i)), "%Y%m%d")))
    return union(tables, fields)

def last_month(start_date, fields=["*"]):
    tables = []
    skip = True
    for i in range(32):
        next_date = start_date + timedelta(-1 * (32 - i))
        # Skip days until we find the same day in the previous month.
        if skip:
            if next_date.day == start_date.day:
                skip = False
            else:
                continue
        tables.append("executive_summary_{}".format(datetime.strftime(next_date, "%Y%m%d")))
    return union(tables, fields)

def get_target_date(start_date, inline_date):
    if inline_date:
        return "'{}'::DATE".format(datetime.strftime(start_date, "%Y-%m-%d"))
    return '%s'

def get_this_period(start_date, mode, fields=["*"]):
    if mode == 'monthly':
        return this_month(start_date, fields)
    else:
        return this_week(start_date, fields)

def get_last_period(start_date, mode, fields=["*"]):
    if mode == 'monthly':
        return last_month(start_date, fields)
    else:
        return last_week(start_date, fields)

def get_targets(start_date, inline_date, mode, fields=["*"]):
    date_param = get_target_date(start_date, inline_date)
    target_param = get_this_period(start_date, mode, fields)
    return date_param, target_param

def client_values(start_date, inline_date, mode):
    template = """SELECT clientid, country, channel, os, new_client, default_client FROM (
 SELECT
  clientid, country, channel, os,
  CASE WHEN profilecreationtimestamp >= {report_date} THEN 1 ELSE 0 END AS new_client,
  CASE WHEN "default" THEN 1 ELSE 0 END AS default_client,
  -- Do not use "rank()" because it gives ties all the same value, so we end
  -- up with many "1" values if we order by country, channel, geo (hence over-
  -- counting the per-client aggregates)
  row_number() OVER (
   -- Use the most recently observed values:
   PARTITION BY clientid ORDER BY "timestamp" DESC
  ) AS clientid_rank
 FROM ({target}) t
) v
WHERE v.clientid_rank = 1"""
    fields = ['clientid', 'country', 'channel', 'os', 'profilecreationtimestamp', '"default"', '"timestamp"']
    report_date, target = get_targets(start_date, inline_date, mode, fields)
    return template.format(report_date=report_date, target=target)


def get_easy_aggregates(start_date, inline_date=False, mode='monthly'):
    template = """SELECT
 country AS geo, channel, os, {report_date} AS "date",
 sum(hours) AS hours,
 -- Count the number of crash documents
 sum(case when doctype = 'crash' then 1 else 0 end) AS crashes,
 sum(google) AS google,
 sum(bing) AS bing,
 sum(yahoo) AS yahoo,
 sum(other) AS other
FROM ({target}) t GROUP BY 1, 2, 3, 4"""
    fields = ['country', 'channel', 'os', 'hours', 'doctype', 'google', 'bing', 'yahoo', 'other']
    report_date, target = get_targets(start_date, inline_date, mode, fields)
    return template.format(report_date=report_date, target=target)

def get_client_aggregates(start_date, inline_date=False, mode='monthly'):
    template = """SELECT
 country AS geo, channel, os, {report_date} AS "date",
 COUNT(*) AS actives,
 SUM(new_client) AS new_clients,
 SUM(default_client) AS "default"
FROM ({client_values}) client_values
GROUP BY 1, 2, 3, 4"""
    report_date = get_target_date(start_date, inline_date)
    return template.format(report_date=report_date, client_values=client_values(start_date, inline_date, mode))

def get_inactives(start_date, inline_date=False, mode='monthly'):
    template = """SELECT country AS geo, channel, os, {report_date} AS "date", COUNT(*) AS inactives FROM (
 SELECT * FROM (
  SELECT
   clientid,
   country,
   channel,
   os,
   -- Do not use "rank()" because it gives tied rows all the same value, so we
   -- end up with many "1" values if we order by country, channel, geo (hence
   -- over-counting the inactives)
   row_number() OVER (
    -- Use the most recently observed values:
    PARTITION BY clientid ORDER BY "timestamp" DESC
   ) AS clientid_rank
  FROM ({last_period}) l WHERE clientid IN (
   SELECT clientid FROM ({last_period}) l EXCEPT SELECT clientid FROM ({this_period}) t
  )
 ) AS ranked
 WHERE ranked.clientid_rank = 1
) t GROUP BY 1, 2, 3, 4"""
    report_date, this_period = get_targets(start_date, inline_date, mode, fields=["clientid"])

    fields = ['clientid', 'country', 'channel', 'os', '"timestamp"']
    last_period = get_last_period(start_date, mode, fields)
    return template.format(report_date=report_date, this_period=this_period, last_period=last_period)

def get_five_of_seven(start_date, inline_date=False, mode='monthly'):
    template = """SELECT country AS geo, channel, os, {report_date} AS "date",
 sum(
  -- For weekly, 5/7 = 0.714 Let's call it 21 days out of the month, which
  -- corresponds to min 21/31 = 0.677, max 21/28 = 0.75
  CASE WHEN num_days >= {fos_days} THEN 1 ELSE 0 END
 ) AS five_of_seven
FROM (
 SELECT
  clientid, country, channel, os,
  -- Number of days on which we received submissions from this client.
  count(distinct "timestamp"::date) AS num_days
 FROM ({target}) t GROUP BY 1, 2, 3, 4
) v GROUP BY 1, 2, 3, 4;"""
    fields = ['clientid', 'country', 'channel', 'os', '"timestamp"']
    report_date, target = get_targets(start_date, inline_date, mode, fields)
    # Using 'format' to insert a value into a query is a no-no, but in this case
    # we can guarantee that it's a known int value.
    fos_days = 5
    if mode == 'monthly':
        fos_days = 21
    return template.format(report_date=report_date, fos_days=fos_days, target=target)

def get_row_key(row):
    return (ne(row["geo"]), ne(row["channel"]), ne(row["os"]), ne(datetime.strftime(row["date"], u"%Y-%m-%d")))

# Replace None with an empty string
def ne(v):
    if v is None:
        return u""
    return unicode(v)

# Replace None with zero
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

summary_tables = None

def main():
    parser = argparse.ArgumentParser(description="Run Executive Report")
    parser.add_argument("--report-start",  help="Start day of the reporting period (YYYYMMDD)", required=True)
    parser.add_argument("--mode",          help="Report mode: weekly or monthly", default="monthly")
    parser.add_argument("--db-url",        help="Database URL to connect to", required=True)
    parser.add_argument("--dry-run",       help="Print out what would happen instead of running queries", action="store_true")
    parser.add_argument("--check-tables",  help="Check that the underlying tables exist when creating UNION queries", action="store_true")
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
        print >> sys.stderr, "Preparing to generate {} report for {}".format(args.mode, args.report_start)

    if args.dry_run:
        print >> sys.stderr, "-- Dry run mode. Printing queries only."
        print "-- Easy aggregates:"
        print get_easy_aggregates(report_start_date, inline_date=True, mode=args.mode)
        print "-- Client aggregates:"
        print get_client_aggregates(report_start_date, inline_date=True, mode=args.mode)
        print "-- Inactives:"
        print get_inactives(report_start_date, inline_date=True, mode=args.mode)
        print "-- Five of Seven:"
        print get_five_of_seven(report_start_date, inline_date=True, mode=args.mode)
        return exit_code

    sd = report_start_date.date()
    with psycopg2.connect(args.db_url) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:

            if args.check_tables:
                if args.verbose:
                    print >> sys.stderr, "Listing existing daily tables..."
                global summary_tables
                summary_tables = []
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'executive_summary_%' ORDER BY 1")
                for row in cursor:
                    summary_tables.append(row["table_name"])

            if not args.skip_easy:
                if args.verbose:
                    print >> sys.stderr, "Generating simple aggregates..."
                cursor.execute(get_easy_aggregates(report_start_date, mode=args.mode), (sd,))
                for row in cursor:
                    # Key fields
                    k = get_row_key(row)
                    v = [0,nz(row["hours"]),0,0,0,0,nz(row["crashes"]),0,nz(row["google"]),nz(row["bing"]),nz(row["yahoo"]),nz(row["other"])]
                    report[k] = v

            if not args.skip_client:
                if args.verbose:
                    print >> sys.stderr, "Generating per-client aggregates..."
                cursor.execute(get_client_aggregates(report_start_date, mode=args.mode), (sd,sd))
                for row in cursor:
                    k = get_row_key(row)
                    v = report.get(k)
                    if v is None:
                        v = [0,0,0,0,0,0,0,0,0,0,0,0]
                    v[ACTIVES] = nz(row["actives"])
                    # total_records = actives + inactives
                    # So we initialize with ACTIVES, and add in INACTIVES below.
                    v[TOTAL_RECORDS] = v[ACTIVES]
                    v[NEW_RECORDS] = nz(row["new_clients"])
                    v[DEFAULT] = nz(row["default"])
                    report[k] = v

            if not args.skip_inactive:
                if args.verbose:
                    print >> sys.stderr, "Generating inactives..."
                cursor.execute(get_inactives(report_start_date, mode=args.mode), (sd,))
                for row in cursor:
                    k = get_row_key(row)
                    v = report.get(k)
                    if v is None:
                        v = [0,0,0,0,0,0,0,0,0,0,0,0]
                    v[INACTIVES] = nz(row["inactives"])
                    # total_records = actives + inactives
                    # So add in INACTIVES.
                    v[TOTAL_RECORDS] += v[INACTIVES]
                    report[k] = v

            if not args.skip_fos:
                if args.verbose:
                    print >> sys.stderr, "Generating five-of-seven..."
                cursor.execute(get_five_of_seven(report_start_date, mode=args.mode), (sd,))
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

    print u"geo,channel,os,date,actives,hours,inactives,new_records,five_of_seven,total_records,crashes,default,google,bing,yahoo,other"
    for k, v in report.iteritems():
        s = [ unicode(kk) for kk in k ]
        for vv in v:
            s.append(unicode(vv))
        print u",".join(s)

    return exit_code

if __name__ == "__main__":
    sys.exit(main())
