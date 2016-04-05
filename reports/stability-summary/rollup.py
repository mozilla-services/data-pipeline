import psycopg2
import os
import sys
import csv
from datetime import datetime, date, timedelta
from utils import S3CompressedWriter
from summarize import summarize

# How many days "back" do we look for activity?
latency_interval = 10

default_bucket = 'telemetry-public-analysis-2'

current_cutoff = date.today() - timedelta(days=1)

def date_range(date, days, cutoff):
    """Iterate from `date` for the next `days`"""
    for d in range(0, days):
        curd = date + timedelta(days=d)
        if curd > cutoff:
            return
        yield curd

def put_counts(db, date):
    from_template = '''
      SELECT
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        reason,
        CASE WHEN subsessionlength < 0 THEN 0
             WHEN subsessionlength > 3600 * 25 THEN 3600 * 25
             ELSE subsessionlength END AS subsessionlength,
        abortsplugin,
        abortscontent,
        abortsgmplugin,
        crashesdetectedplugin,
        pluginhangs,
        crashesdetectedcontent,
        crashesdetectedgmplugin,
        crashsubmitattemptmain,
        crashsubmitattemptcontent,
        crashsubmitattemptplugin,
        crashsubmitsuccessmain,
        crashsubmitsuccesscontent,
        crashsubmitsuccessplugin
      FROM {tablename}
      WHERE subsessiondate = %(day)s'''

    final_template = '''
      SELECT
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        SUM(CASE WHEN reason = 'aborted-session' THEN 1 ELSE 0 END),
        SUM(subsessionlength),
        SUM(abortsplugin),
        SUM(abortscontent),
        SUM(abortsgmplugin),
        SUM(crashesdetectedplugin),
        SUM(pluginhangs),
        SUM(crashesdetectedcontent),
        SUM(crashesdetectedgmplugin),
        SUM(crashsubmitattemptmain),
        SUM(crashsubmitattemptcontent),
        SUM(crashsubmitattemptplugin),
        SUM(crashsubmitsuccessmain),
        SUM(crashsubmitsuccesscontent),
        SUM(crashsubmitsuccessplugin)
      FROM ( {unionclause} )
      GROUP BY
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country'''

    union_query = ' UNION ALL '.join(
        from_template.format(tablename='main_summary_{}'.format(d.strftime('%Y%m%d')))
        for d in date_range(date - timedelta(days=1), latency_interval + 1, current_cutoff))

    final_query = final_template.format(unionclause=union_query)

    cur = db.cursor()
    cur.execute(final_query, {'day': date})

    path = 'stability-rollups/{year}/{date}-main.csv.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'))

    with S3CompressedWriter(default_bucket, path) as fd:
        outcsv = csv.writer(fd)

        outcsv.writerow((
            'buildversion',
            'buildid',
            'buildarchitecture',
            'channel',
            'os',
            'osversion',
            'osservicepackmajor',
            'osservicepackminor',
            'locale',
            'activeexperimentid',
            'activeexperimentbranch',
            'country',
            'abortedsessioncount',
            'subsessionlengths',
            'abortsplugin',
            'abortscontent',
            'abortsgmplugin',
            'crashesdetectedplugin',
            'pluginhangs',
            'crashesdetectedcontent',
            'crashesdetectedgmplugin',
            'crashsubmitattemptmain',
            'crashsubmitattemptcontent',
            'crashsubmitattemptplugin',
            'crashsubmitsuccessmain',
            'crashsubmitsuccesscontent',
            'crashsubmitsuccessplugin'))

        for r in cur:
            outcsv.writerow(r)

    db.commit()

def put_actives(db, date, weekly):
    if weekly:
        where_clause = '''subsessiondate <= %(day)s::date AND subsessiondate > %(day)s::date - '1 week'::interval'''
    else:
        where_clause = '''subsessiondate = %(day)s'''

    from_template = '''
      SELECT
        subsessiondate,
        clientid,
        buildversion,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country
      FROM {tablename}
      WHERE {whereclause}'''

    final_template = '''
      SELECT
        buildversion,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        activedays,
        COUNT(*)
      FROM (
        SELECT
          buildversion,
          buildarchitecture,
          channel,
          os,
          osversion,
          osservicepackmajor,
          osservicepackminor,
          locale,
          activeexperimentid,
          activeexperimentbranch,
          country,
          DENSE_RANK() OVER (PARTITION BY clientid ORDER BY subsessiondate ASC) AS activedays,
          ROW_NUMBER() OVER (PARTITION BY clientid ORDER BY subsessiondate DESC) AS rownumber
        FROM ( {unionclause} )
      )
      WHERE rownumber = 1
      GROUP BY
        buildversion,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        activedays'''

    if weekly:
        dates = date_range(date - timedelta(days=8), latency_interval + 1 + 7, current_cutoff)
    else:
        dates = date_range(date - timedelta(days=1), latency_interval + 1, current_cutoff)

    union_query = ' UNION ALL '.join(
        from_template.format(tablename='main_summary_{}'.format(d.strftime('%Y%m%d')), whereclause=where_clause)
        for d in dates)

    final_query = final_template.format(unionclause=union_query)

    cur = db.cursor()
    cur.execute(final_query, {'day': date})

    if weekly:
        segment = 'weekly'
    else:
        segment = 'daily'

    path = 'stability-rollups/{year}/{date}-active-{segment}.csv.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'), segment=segment)

    with S3CompressedWriter(default_bucket, path) as fd:
        outcsv = csv.writer(fd)

        outcsv.writerow((
            'buildversion',
            'buildarchitecture',
            'channel',
            'os',
            'osversion',
            'osservicepackmajor',
            'osservicepackminor',
            'locale',
            'activeexperimentid',
            'activeexperimentbranch',
            'country',
            'active_days',
            'active_users'))

        for r in cur:
            outcsv.writerow(r)

    db.commit()

def put_crashes(db, date):
    from_template = '''
      SELECT
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        hascrashenvironment
      FROM {tablename}
      WHERE crashdate = %(day)s'''

    final_template = '''
      SELECT
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        hascrashenvironment,
        COUNT(*)
      FROM ( {unionclause} )
      GROUP BY
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        hascrashenvironment'''

    union_query = ' UNION ALL '.join(
        from_template.format(tablename='crash_summary_{}'.format(d.strftime('%Y%m%d')))
        for d in date_range(date - timedelta(days=1), latency_interval + 1, current_cutoff))

    final_query = final_template.format(unionclause=union_query)

    cur = db.cursor()
    cur.execute(final_query, {'day': date})

    path = 'stability-rollups/{year}/{date}-crashes.csv.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'))

    with S3CompressedWriter(default_bucket, path) as fd:
        outcsv = csv.writer(fd)

        outcsv.writerow((
            'buildversion',
            'buildid',
            'buildarchitecture',
            'channel',
            'os',
            'osversion',
            'osservicepackmajor',
            'osservicepackminor',
            'locale',
            'activeexperimentid',
            'activeexperimentbranch',
            'country',
            'hascrashenvironment',
            'crashes'))

        for r in cur:
            outcsv.writerow(r)

    db.commit()

def generate_summary_table(db, date):
    final_template = '''
      CREATE TABLE usage_aggregate_{dateyyyymmdd}
      DISTSTYLE EVEN
      COMPOUND SORTKEY (subsessiondate, channel)
      AS (
      SELECT
        %(date)s::date,
        subsessiondate,
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country,
        SUM(abortsmain) AS abortsmain,
        SUM(subsessionlength) AS subsessionlength,
        SUM(abortsplugin) AS abortsplugin,
        SUM(abortscontent) AS abortscontent,
        SUM(abortsgmplugin) AS abortsgmplugin,
        SUM(crashesdetectedmain) AS crashesdetectedmain,
        SUM(crashesdetectedplugin) AS crashesdetectedplugin,
        SUM(pluginhangs) AS pluginhangs,
        SUM(crashesdetectedcontent) AS crashesdetectedcontent,
        SUM(crashesdetectedgmplugin) AS crashesdetectedgmplugin,
        SUM(crashsubmitattemptmain) AS crashsubmitattemptmain,
        SUM(crashsubmitattemptcontent) AS crashsubmitattemptcontent,
        SUM(crashsubmitattemptplugin) AS crashsubmitattemptplugin,
        SUM(crashsubmitsuccessmain) AS crashsubmitsuccessmain,
        SUM(crashsubmitsuccesscontent) AS crashsubmitsuccesscontent,
        SUM(crashsubmitsuccessplugin) AS crashsubmitsuccessplugin
      FROM (
        SELECT
          subsessiondate,
          buildversion,
          buildid,
          buildarchitecture,
          channel,
          os,
          osversion,
          osservicepackmajor,
          osservicepackminor,
          locale,
          activeexperimentid,
          activeexperimentbranch,
          country,
          CASE WHEN subsessionlength < 0 THEN 0
               WHEN subsessionlength > 3600 * 25 THEN 3600 * 25
               ELSE subsessionlength END AS subsessionlength,
          (CASE WHEN reason = 'aborted-session' THEN 1 ELSE 0 END) AS abortsmain,
          abortsplugin,
          abortscontent,
          abortsgmplugin,
          0 AS crashesdetectedmain,
          crashesdetectedplugin,
          pluginhangs,
          crashesdetectedcontent,
          crashesdetectedgmplugin,
          crashsubmitattemptmain,
          crashsubmitattemptcontent,
          crashsubmitattemptplugin,
          crashsubmitsuccessmain,
          crashsubmitsuccesscontent,
          crashsubmitsuccessplugin
        FROM main_summary_{dateyyyymmdd}
        UNION ALL
        SELECT
          crashdate,
          buildversion,
          buildid,
          buildarchitecture,
          channel,
          os,
          osversion,
          osservicepackmajor,
          osservicepackminor,
          locale,
          activeexperimentid,
          activeexperimentbranch,
          country,
          0,
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0
        FROM crash_summary_{dateyyyymmdd}
      )
      GROUP BY
        subsessiondate,
        buildversion,
        buildid,
        buildarchitecture,
        channel,
        os,
        osversion,
        osservicepackmajor,
        osservicepackminor,
        locale,
        activeexperimentid,
        activeexperimentbranch,
        country
    )'''

    final_query = final_template.format(dateyyyymmdd=date.strftime('%Y%m%d'))

    cur = db.cursor()
    cur.execute("DROP TABLE IF EXISTS usage_aggregate_{dateyyyymmdd} CASCADE".format(dateyyyymmdd=date.strftime('%Y%m%d')))
    cur.execute(final_query, {'date': date})
    cur.execute("GRANT SELECT ON usage_aggregate_{dateyyyymmdd} TO read_only".format(dateyyyymmdd=date.strftime('%Y%m%d')))
    db.commit()

def rebuild_summary_view(db):
    cur = db.cursor()
    cur.execute('''SELECT table_name
                   FROM information_schema.tables
                   WHERE table_name LIKE 'usage_aggregate_%' ''')
    tablenames = [tablename for tablename, in cur.fetchall()]

    cur.execute('''
    CREATE OR REPLACE VIEW usage_aggregate AS {unionquery}
    '''.format(unionquery=" UNION ALL ".join("SELECT * FROM {}".format(tablename) for tablename in tablenames)))

    cur.execute('''GRANT SELECT ON usage_aggregate TO read_only''')

    db.commit()

weekday_saturday = 5
def put_daily(cur, date):
    put_counts(cur, date)
    put_actives(cur, date, False)
    #if date.weekday() == weekday_saturday:
    #    put_actives(cur, date, True)
    put_crashes(cur, date)
    summarize(date)

if __name__ == '__main__':
    import itertools
    import getpass
    from optparse import OptionParser

    p = OptionParser("usage: %prog [options] [YYYYMMDD-startdate] [YYYYMMDD-enddate]")
    p.add_option("-d", "--database", dest="connection", default=None,
                 help="PSQL database connection string")
    p.add_option("--latency", dest="latency", default=True,
                 action="store_true",
                 help="Recompute back through the latency period.")
    p.add_option("--no-latency", dest="latency", action="store_false",
                 help="Recompute the specified days only.")
    p.add_option("--dbsummary", dest="do_summarytable", action="store_true",
                 default=True)
    p.add_option("--no-dbsummary", dest="do_summarytable", action="store_false")
    p.add_option("--csv", dest="do_csv", action="store_true",
                 default=True)
    p.add_option("--no-csv", dest="do_csv", action="store_false")
    opts, args = p.parse_args()

    if opts.connection is None:
        opts.connection = getpass.getpass("Database connection string: ")

    if len(args) == 0:
        start_date = date.today() - timedelta(days=1)
        end_date = start_date
    elif len(args) == 1:
        start_date = datetime.strptime(args[0], '%Y%m%d').date()
        end_date = start_date
    elif len(args) == 2:
        start_date = datetime.strptime(args[0], '%Y%m%d').date()
        end_date = datetime.strptime(args[1], '%Y%m%d').date()
    else:
        p.print_help()
        sys.exit(1)

    end_date = min(end_date, current_cutoff)
    if start_date > end_date:
        print >>sys.stderr, "No data to process: {} - {}".format(start_date, end_date)
        sys.exit(1)

    conn = psycopg2.connect(opts.connection)

    # Summary table, then CSV
    if opts.do_summarytable:
        for i in itertools.count(0):
            d = start_date + timedelta(days=i)
            if d > end_date:
                break
            print "Generating summary table: %s" % (d,)
            generate_summary_table(conn, d)
        print "Rebuilding summary view"
        rebuild_summary_view(conn)

    if opts.do_csv:
        if opts.latency:
            csv_start_date = start_date - timedelta(days=latency_interval)
        else:
            csv_start_date = start_date

        for i in itertools.count(0):
            d = csv_start_date + timedelta(days=i)
            if d > end_date:
                break
            print "Generating CSV rollups: %s" % (d,)
            put_daily(conn, d)

    print "done"
