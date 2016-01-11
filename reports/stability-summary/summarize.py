import csv
import json
from utils import S3CompressedReader, S3CompressedWriter, HeaderCSVReader
from collections import defaultdict, Counter
from itertools import izip, count

default_bucket = 'telemetry-public-analysis-2'

prop_list = (
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
    'crashsubmitsuccessplugin')

class Counts(object):
    def __init__(self):
        self._counts = [0] * len(prop_list)
        self.crashes = 0

    def increment(self, i, v):
        self._counts[i] += v

    def final(self, **kwargs):
        d = dict(izip(prop_list, self._counts))
        d.update(kwargs)
        d['crashesdetectedmain'] = self.crashes
        return d

def nullint(v):
    if v == '':
        return 0
    return int(v)

def summarize(date):
    """
    read the large CSV file produced by rollup.put_counts and
    rollup.put_crashes into a smaller summary JSON format for quick overview
    graphing.
    """

    counts = defaultdict(Counts)

    counts_path = 'stability-rollups/{year}/{date}-main.csv.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'))
    csvheaders, reader = HeaderCSVReader(
        S3CompressedReader(default_bucket, counts_path))
    key_indexes = [csvheaders.index(prop)
                   for prop in ('channel', 'buildid', 'os')]
    csv_indexes = [(csvheaders.index(prop), propidx)
                   for propidx, prop in izip(count(), prop_list)]
    for row in reader:
        key = tuple(row[idx] for idx in key_indexes)
        counter = counts[key]
        for csvidx, propidx in csv_indexes:
            counter.increment(propidx, nullint(row[csvidx]))

    crashes_path = 'stability-rollups/{year}/{date}-crashes.csv.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'))
    csvheaders, reader = HeaderCSVReader(
        S3CompressedReader(default_bucket, crashes_path))
    key_indexes = [csvheaders.index(prop)
                   for prop in ('channel', 'buildid', 'os')]
    for row in reader:
        key = tuple(row[idx] for idx in key_indexes)
        counts[key].crashes += nullint(row[-1])

    summary_path = 'stability-rollups/{year}/{date}-summary.json.gz'.format(
        year=date.year, date=date.strftime('%Y%m%d'))
    with S3CompressedWriter(default_bucket, summary_path) as fd:
        json.dump([c.final(channel=channel, buildid=buildid, os=os)
                   for (channel, buildid, os), c in counts.iteritems()], fd)

if __name__ == '__main__':
    import sys
    from datetime import date, timedelta
    start = date(2015, 11, 5)
    end = date(2015, 11, 30)
    for i in count():
        d = start + timedelta(days=i)
        if d > end:
            break
        summarize(d)
